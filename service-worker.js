const CACHE_NAME = "aurax-shell-v2";
const CORE_ASSETS = [
  "./",
  "./index.html",
  "./styles.css",
  "./app.js",
  "./auth.js",
  "./firebase-config.js",
  "./manifest.webmanifest",
  "./icons/icon.svg",
  "./icons/icon-192.svg",
  "./icons/icon-512.svg",
  "./icons/icon-192-maskable.svg",
  "./icons/icon-512-maskable.svg",
  "./views/dashboard-view.html",
  "./views/aurastats-view.html",
  "./views/auraview-view.html",
  "./views/auravoter-view.html",
  "./views/bank-view.html",
  "./views/admin-view.html",
  "./views/profile-view.html"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(CORE_ASSETS))
      .catch(() => Promise.resolve())
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.filter((key) => !key.includes("aurax"))
          .map((key) => caches.delete(key))
      );
    })
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  if (url.pathname.includes("firebaseio.com")) {
    event.respondWith(fetch(request).catch(() => caches.match(request)));
    return;
  }

  event.respondWith(
    caches.match(request).then((cached) => {
      const fetchPromise = fetch(request)
        .then((response) => {
          if (!response || response.status !== 200) return response;
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, clone));
          return response;
        })
        .catch(() => cached || caches.match("./index.html"));

      return cached && !url.pathname.includes(".html") 
        ? cached 
        : fetchPromise;
    })
  );
});
