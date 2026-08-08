/* Trust Me Bro — minimal PWA service worker.
   Network-first for the app shell (so updates always flow when online),
   cache fallback for offline. NEVER touches the API or any non-GET request,
   so the streaming /api/research POST is unaffected. */
const CACHE = "tmb-v1";
const SHELL = ["/", "/index.html", "/manifest.webmanifest", "/apple-touch-icon.png", "/icon-192.png", "/icon-512.png"];

self.addEventListener("install", (e) => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL).catch(() => {})));
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  const req = e.request;
  if (req.method !== "GET") return;                    // leave POST /api/research alone
  const url = new URL(req.url);
  if (url.pathname.startsWith("/api/")) return;         // never cache the research API
  if (url.origin !== location.origin) return;           // don't touch cross-origin

  if (req.mode === "navigate") {
    // Network-first: always get the freshest app when online; fall back to cached shell offline.
    e.respondWith(
      fetch(req)
        .then((res) => { const copy = res.clone(); caches.open(CACHE).then((c) => c.put("/index.html", copy)); return res; })
        .catch(() => caches.match("/index.html"))
    );
    return;
  }
  // Cache-first for static assets (icons/manifest).
  e.respondWith(
    caches.match(req).then((hit) =>
      hit || fetch(req).then((res) => {
        if (res && res.ok) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
        return res;
      }).catch(() => hit)
    )
  );
});
