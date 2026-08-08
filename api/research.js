/* ============================================================
   Trust Me Bro — serverless research proxy (Vercel Node function)
   STREAMING version.
   ------------------------------------------------------------
   The Anthropic API key lives ONLY here (env var ANTHROPIC_API_KEY),
   read at runtime, never sent to the browser or the client bundle.

   POST /api/research { question, mode }  ->  streams newline-delimited
   JSON events (application/x-ndjson) so the client renders progressively:
     {"type":"status","phase":"search|reading|writing","detail":"..."}
     {"type":"section","kind":"bottom_line|evidence|study|takeaway|disagreements|not_supported|sources", ...}
     {"type":"done","result":{...},"cached":bool}
     {"type":"error","error":"..."}

   Abuse guards:
   - DURABLE rate limiting via Upstash Redis REST (per-IP hourly cap + a
     global daily API-spend ceiling). Enforced across cold starts and
     instances; falls back to per-instance memory only if Redis is not
     configured or is temporarily unreachable.
   - Length cap, bounded cost. Guard order: method -> body -> empty ->
     length -> rate -> key.

   pause_turn: when a long web_search run pauses the turn, the assistant's
   actual content blocks (including the search results) are replayed back
   and the turn resumes to completion — instead of restarting research from
   scratch (wasted cost) or caching a truncated answer.
   ============================================================ */

function envInt(name, dflt) { const v = parseInt(process.env[name], 10); return Number.isFinite(v) ? v : dflt; }

const MODEL = process.env.CORPUS_MODEL || "claude-sonnet-4-6";
const MAX_QUESTION_LEN = 600;
const PER_IP_PER_HOUR = envInt("CORPUS_PER_IP_HOUR", 15);
const DAILY_BUDGET_CENTS = envInt("CORPUS_DAILY_BUDGET_CENTS", 1000); // global API-spend ceiling, default $10/day
const COST_CENTS = { quick: envInt("CORPUS_COST_QUICK_CENTS", 7), deep: envInt("CORPUS_COST_DEEP_CENTS", 13) };
const WINDOW_MS = 60 * 60 * 1000;
const MAX_PAUSE_ITERS = 8;
const SEARCHES = { quick: 3, deep: 6 };
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const DEBUG = !!process.env.CORPUS_DEBUG;

const LEVELS = ["Strong", "Moderate", "Emerging", "Weak"];
const STUDY_TYPES = ["Meta-analysis", "Systematic review", "RCT", "Observational", "Animal", "Review"];

const SYSTEM_PROMPT = `You are Trust Me Bro, a rigorous evidence synthesist. Answer the user's health/science question strictly from the real, peer-reviewed literature you retrieve with the web_search tool — never from memory, and never from social-media-style claims.

SEARCH: Use web_search to find real studies. Prioritise PubMed, Europe PMC, ClinicalTrials.gov, and peer-reviewed journals. Search for systematic reviews and meta-analyses first (top of the evidence pyramid), then RCTs, then large observational studies. Prefer recent, higher-quality designs. Be efficient — a few well-chosen searches, not many.

NON-NEGOTIABLE RULES:
1. Only cite real papers that appear in your web_search results. Never invent a title, journal, year, sample size, finding, or link.
2. Every study's "url" MUST be a real link from your search results (a DOI link like https://doi.org/... or a PubMed/PMC/journal/ClinicalTrials page). If you don't have a real link, omit that study.
3. If the literature doesn't answer the question well, say so plainly in the bottom_line and grade the evidence accordingly, returning few or no studies rather than padding.
4. Prioritise the strongest, most recent designs. When a widely-repeated claim is NOT well supported, say so.
5. This is a research summary, not medical advice.

Grade overall evidence as exactly one of: Strong, Moderate, Emerging, Weak.
Classify each study's design as exactly one of: Meta-analysis, Systematic review, RCT, Observational, Animal, Review.

OUTPUT FORMAT — after you finish searching, output your answer as NEWLINE-DELIMITED JSON: one compact JSON object per line, and NOTHING ELSE (no prose, no markdown, no code fences). Emit the lines in exactly this order:
{"t":"bottom_line","v":"plain-English answer, 2-3 sentences; lead with what the evidence supports"}
{"t":"evidence","level":"Strong|Moderate|Emerging|Weak","rationale":"one line on why"}
{"t":"study","title":"...","journal":"...","year":2024,"study_type":"Meta-analysis","sample_size":"e.g. 12 RCTs, n=450","finding":"the specific result with effect sizes/direction","url":"real link"}
(repeat one {"t":"study",...} line per key study, strongest designs first, 4-8 studies)
{"t":"takeaway","v":"dose/protocol/what it means in practice"}      (omit this line entirely if not applicable)
{"t":"disagreements","v":"genuine expert disagreement"}             (omit if none)
{"t":"not_supported","v":"common claims the evidence does NOT back"} (omit if none)
{"t":"sources","v":"which databases/journals the evidence draws on; note if coverage was thin"}

Each line must be valid JSON on its own. Do not wrap the output in an array or in code fences.`;

/* ---- durable rate limiting via Upstash Redis REST ----
   Accepts either the Vercel-injected KV_* names or native UPSTASH_* names. */
const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || "";
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || "";
const REDIS_OK = !!(UPSTASH_URL && UPSTASH_TOKEN);

async function redisPipeline(commands) {
  const r = await fetch(UPSTASH_URL.replace(/\/$/, "") + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + UPSTASH_TOKEN, "content-type": "application/json" },
    body: JSON.stringify(commands)
  });
  if (!r.ok) throw new Error("Upstash " + r.status);
  return r.json(); // -> [{result:...}|{error:...}, ...]
}

/* ---- per-instance memory fallback (best-effort; only if Redis absent/down) ---- */
const ipHits = new Map();
let memBudgetCents = 0, memBudgetDay = -1;
function memoryLimit(ip, cost) {
  const now = Date.now();
  const day = Math.floor(now / 86400000);
  if (day !== memBudgetDay) { memBudgetDay = day; memBudgetCents = 0; }
  memBudgetCents += cost;
  if (memBudgetCents > DAILY_BUDGET_CENTS) return { blocked: true, msg: "Daily research budget reached — please try again tomorrow." };
  const arr = (ipHits.get(ip) || []).filter((t) => now - t < WINDOW_MS);
  if (arr.length >= PER_IP_PER_HOUR) { ipHits.set(ip, arr); return { blocked: true, msg: `Hourly limit reached (${PER_IP_PER_HOUR}/hour). Try again later.` }; }
  arr.push(now); ipHits.set(ip, arr);
  if (ipHits.size > 5000) ipHits.clear();
  return { blocked: false };
}

async function checkLimit(ip, mode) {
  const cost = COST_CENTS[mode] || COST_CENTS.quick;
  if (!REDIS_OK) { if (DEBUG) console.error("[corpus] rate limit: memory fallback (Redis not configured)"); return memoryLimit(ip, cost); }
  const now = Date.now();
  const hourBucket = Math.floor(now / 3600000);
  const dayBucket = Math.floor(now / 86400000);
  const ipKey = `corpus:ip:${ip}:${hourBucket}`;
  const budgetKey = `corpus:budget:${dayBucket}`;
  try {
    const res = await redisPipeline([
      ["INCRBY", budgetKey, cost],
      ["EXPIRE", budgetKey, 90000, "NX"],           // ~25h, outlives the day bucket
      ["INCR", ipKey],
      ["EXPIRE", ipKey, Math.floor(WINDOW_MS / 1000), "NX"]
    ]);
    const spent = Number((res[0] && res[0].result) || 0);
    const ipCount = Number((res[2] && res[2].result) || 0);
    if (DEBUG) console.error(`[corpus] rate limit ip=${ip} ipCount=${ipCount}/${PER_IP_PER_HOUR} spent=${spent}/${DAILY_BUDGET_CENTS}c`);
    if (spent > DAILY_BUDGET_CENTS) return { blocked: true, msg: "Daily research budget reached — please try again tomorrow." };
    if (ipCount > PER_IP_PER_HOUR) return { blocked: true, msg: `Hourly limit reached (${PER_IP_PER_HOUR}/hour). Try again later.` };
    return { blocked: false };
  } catch (e) {
    // Redis blip: fall back to per-instance memory rather than DoS'ing the app.
    if (DEBUG) console.error("[corpus] rate limit: Redis error, memory fallback:", e && e.message);
    return memoryLimit(ip, cost);
  }
}

/* ---- in-memory result cache (best-effort; durable across a warm instance) ---- */
const cache = new Map(); // normQ -> { result, expires }
function normQ(q) { return q.toLowerCase().replace(/\s+/g, " ").replace(/[?!.\s]+$/g, "").trim(); }
function cacheGet(q) {
  const key = normQ(q);
  const e = cache.get(key);
  if (e && e.expires > Date.now()) return e.result;
  if (e) cache.delete(key);
  return null;
}
function cacheSet(q, result) {
  cache.set(normQ(q), { result, expires: Date.now() + CACHE_TTL_MS });
  if (cache.size > 500) { const k = cache.keys().next().value; cache.delete(k); }
}

/* ---- section normalization ---- */
function clampLevel(l) {
  const t = String(l || "").trim().toLowerCase();
  for (const L of LEVELS) if (L.toLowerCase() === t) return L;
  if (t.startsWith("prelim")) return "Emerging";
  return "Moderate";
}
function clampType(t) {
  const s = String(t || "").trim().toLowerCase();
  for (const T of STUDY_TYPES) if (T.toLowerCase() === s) return T;
  if (s.includes("meta")) return "Meta-analysis";
  if (s.includes("systematic")) return "Systematic review";
  if (s.includes("random") || s === "rct") return "RCT";
  if (s.includes("cohort") || s.includes("observ") || s.includes("cross")) return "Observational";
  if (s.includes("mice") || s.includes("rat") || s.includes("animal") || s.includes("vitro")) return "Animal";
  return "Review";
}
function tryParse(line) {
  let s = String(line || "").trim();
  if (!s) return null;
  s = s.replace(/^`+|`+$/g, "").trim();
  if (s[0] !== "{") { const i = s.indexOf("{"); if (i === -1) return null; s = s.slice(i); }
  try { return JSON.parse(s); } catch (e) { return null; }
}
function str(v) { return v == null ? "" : String(v).trim(); }

module.exports = async function handler(req, res) {
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  // CORS for the native app shell (Capacitor origin) + the web app. Endpoint is public anyway,
  // so reflecting these specific origins adds capability without weakening anything.
  const origin = (req.headers.origin || "").toString();
  const ALLOWED_ORIGINS = ["capacitor://localhost", "https://trustmebro-research.vercel.app", "http://localhost", "http://localhost:8801"];
  if (ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "content-type");
    res.setHeader("Access-Control-Max-Age", "86400");
  }
  if (req.method === "OPTIONS") { res.status(204).end(); return; }
  if (req.method !== "POST") { res.status(405).json({ error: "Method not allowed." }); return; }

  let body = req.body;
  if (typeof body === "string") { try { body = JSON.parse(body); } catch (e) { body = {}; } }
  const question = ((body && body.question) || "").toString().trim();
  const mode = (body && body.mode) === "deep" ? "deep" : "quick";

  if (!question) { res.status(400).json({ error: "Please enter a question." }); return; }
  if (question.length > MAX_QUESTION_LEN) { res.status(400).json({ error: `Question too long (max ${MAX_QUESTION_LEN} characters).` }); return; }

  const ip = ((req.headers["x-forwarded-for"] || "").split(",")[0].trim()) || (req.socket && req.socket.remoteAddress) || "unknown";
  const lim = await checkLimit(ip, mode);
  if (lim.blocked) { res.status(429).json({ error: lim.msg }); return; }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) { res.status(500).json({ error: "Server not configured: ANTHROPIC_API_KEY is missing. Set it in Vercel → Project → Settings → Environment Variables." }); return; }

  // Begin streaming (NDJSON).
  res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("X-Accel-Buffering", "no");
  const send = (obj) => { try { res.write(JSON.stringify(obj) + "\n"); } catch (e) {} };

  // Cache hit → return instantly.
  const hit = cacheGet(question);
  if (hit) {
    send({ type: "status", phase: "writing", detail: "from cache" });
    send({ type: "done", result: hit, cached: true });
    res.end();
    return;
  }

  try {
    const result = await streamResearch(question, apiKey, SEARCHES[mode], send);
    // Only cache a COMPLETE answer — never a truncated/paused-out one.
    if (result && result.bottom_line && result._complete) cacheSet(question, result);
    if (result) delete result._complete;
    send({ type: "done", result });
  } catch (e) {
    send({ type: "error", error: (e && (e.friendly || e.message)) || "Research failed." });
  }
  res.end();
};

/* ---- reconstruct a streamed content block for pause_turn replay ---- */
function reconstructBlock(b) {
  if (!b) return null;
  if (b.type === "text") { return b.text ? { type: "text", text: b.text } : null; }
  if (b.type === "server_tool_use") {
    let input = {};
    try { input = b.jsonBuf ? JSON.parse(b.jsonBuf) : {}; } catch (e) { input = {}; }
    return { type: "server_tool_use", id: b.raw.id, name: b.raw.name, input };
  }
  if (b.type === "web_search_tool_result") {
    return { type: "web_search_tool_result", tool_use_id: b.raw.tool_use_id, content: b.raw.content };
  }
  return b.raw && b.raw.type ? b.raw : null;
}

/* ---- drive the Anthropic streaming API; forward status + sections ---- */
async function streamResearch(question, apiKey, maxUses, send) {
  const tools = [{ type: "web_search_20260209", name: "web_search", max_uses: maxUses }];
  let messages = [{ role: "user", content: question }];

  const result = { bottom_line: "", evidence_quality: { level: "Moderate", rationale: "" }, key_studies: [], practical_takeaway: "", disagreements: "", not_supported: "", sources_note: "", _complete: false };
  let sawWriting = false;
  let textBuf = "";   // persists across pause_turn resumes so a line split by a pause still parses

  const handleLine = (line) => {
    const p = tryParse(line);
    if (!p) return;
    if (p.t === "bottom_line") { result.bottom_line = str(p.v); send({ type: "section", kind: "bottom_line", v: result.bottom_line }); }
    else if (p.t === "evidence") { result.evidence_quality = { level: clampLevel(p.level), rationale: str(p.rationale) }; send({ type: "section", kind: "evidence", level: result.evidence_quality.level, rationale: result.evidence_quality.rationale }); }
    else if (p.t === "study") {
      const s = { title: str(p.title), journal: str(p.journal), year: p.year || "", study_type: clampType(p.study_type), sample_size: str(p.sample_size), finding: str(p.finding), url: str(p.url) };
      if (s.title) { result.key_studies.push(s); send({ type: "section", kind: "study", study: s }); }
    }
    else if (p.t === "takeaway") { result.practical_takeaway = str(p.v); send({ type: "section", kind: "takeaway", v: result.practical_takeaway }); }
    else if (p.t === "disagreements") { result.disagreements = str(p.v); send({ type: "section", kind: "disagreements", v: result.disagreements }); }
    else if (p.t === "not_supported") { result.not_supported = str(p.v); send({ type: "section", kind: "not_supported", v: result.not_supported }); }
    else if (p.t === "sources") { result.sources_note = str(p.v); send({ type: "section", kind: "sources", v: result.sources_note }); }
  };

  for (let iter = 0; iter < MAX_PAUSE_ITERS; iter++) {
    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: MODEL, max_tokens: 4096, system: SYSTEM_PROMPT, tools, messages, stream: true })
    });
    if (!resp.ok) {
      let detail = ""; try { const j = await resp.json(); detail = (j && j.error && j.error.message) || ""; } catch (e) {}
      if (DEBUG) console.error("[corpus] upstream error", resp.status, detail);
      if (resp.status === 401) throw { friendly: "The research service isn't configured correctly (bad API key)." };
      if (resp.status === 429) throw { friendly: "Rate-limited by the upstream API. Try again shortly." };
      throw { friendly: "Upstream error " + resp.status + (detail ? ": " + detail : "") };
    }

    const reader = resp.body.getReader();
    const dec = new TextDecoder();
    let sse = "";
    const blocks = {};   // index -> reconstruction seed
    const order = [];    // block indexes in arrival order
    let stopReason = null;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      sse += dec.decode(value, { stream: true });
      const events = sse.split("\n\n");
      sse = events.pop() || "";
      for (const ev of events) {
        const dataLine = ev.split("\n").find((l) => l.startsWith("data:"));
        if (!dataLine) continue;
        let data; try { data = JSON.parse(dataLine.slice(5).trim()); } catch (e) { continue; }

        if (data.type === "content_block_start") {
          const cb = data.content_block || {};
          if (!(data.index in blocks)) order.push(data.index);
          blocks[data.index] = { raw: cb, type: cb.type, name: cb.name, jsonBuf: "", text: cb.type === "text" ? (cb.text || "") : "" };
          if (cb.type === "server_tool_use" && cb.name === "web_search") send({ type: "status", phase: "search" });
          else if (cb.type === "web_search_tool_result") send({ type: "status", phase: "reading" });
          else if (cb.type === "text" && !sawWriting) { sawWriting = true; send({ type: "status", phase: "writing" }); }
        } else if (data.type === "content_block_delta") {
          let b = blocks[data.index];
          if (!b) { b = blocks[data.index] = { raw: { type: "text" }, type: "text", jsonBuf: "", text: "" }; order.push(data.index); }
          const d = data.delta || {};
          if (d.type === "input_json_delta") { b.jsonBuf += (d.partial_json || ""); }
          else if (d.type === "text_delta") {
            b.text += d.text || "";
            textBuf += d.text || "";
            let nl;
            while ((nl = textBuf.indexOf("\n")) !== -1) { const line = textBuf.slice(0, nl); textBuf = textBuf.slice(nl + 1); if (line.trim()) handleLine(line); }
          }
        } else if (data.type === "content_block_stop") {
          const b = blocks[data.index];
          if (b && b.type === "server_tool_use") {
            try { const q = JSON.parse(b.jsonBuf || "{}"); if (q && q.query) send({ type: "status", phase: "search", detail: String(q.query).slice(0, 90) }); } catch (e) {}
          }
        } else if (data.type === "message_delta") {
          if (data.delta && data.delta.stop_reason) stopReason = data.delta.stop_reason;
        }
      }
    }

    if (stopReason === "pause_turn") {
      // Replay the assistant's real content (search calls + results) so the turn RESUMES
      // where it left off, instead of restarting research from scratch.
      const assistantContent = order.map((i) => reconstructBlock(blocks[i])).filter(Boolean);
      if (DEBUG) console.error(`[corpus] pause_turn (iter ${iter}) — resuming with ${assistantContent.length} blocks; bottom_line=${!!result.bottom_line}`);
      if (assistantContent.length) messages.push({ role: "assistant", content: assistantContent });
      continue;
    }

    // Natural stop (end_turn / max_tokens / etc.) — flush any trailing partial line.
    if (textBuf.trim()) { handleLine(textBuf); textBuf = ""; }
    result._complete = true;
    if (DEBUG) console.error(`[corpus] done stop=${stopReason} complete studies=${result.key_studies.length}`);
    break;
  }

  if (!result.bottom_line) throw { friendly: "Couldn't produce a structured answer. Try rephrasing the question." };
  return result;
}
