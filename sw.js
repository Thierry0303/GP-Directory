// sw.js — London GP Directory service worker
//
// Required for PWA installability (and therefore for TWA/App-store
// wrapping). Strategy:
//   - Static shell (index.html, this file's own dependencies): cache-first,
//     so the app opens instantly even on a poor connection.
//   - /data.json (the live practice data): network-first, falling back to
//     cache when offline. This data refreshes daily via the GitHub Actions
//     pipeline — the app should always try to get the freshest copy, but
//     shouldn't go blank if the user opens it with no signal.
//   - Everything else (borough pages, practice pages, etc.): network-first
//     with cache fallback, same reasoning.

const CACHE_NAME = 'londongp-v1';
const PRECACHE_URLS = [
  '/',
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png',
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(PRECACHE_URLS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const { request } = event;

  // Only handle GET requests within our own origin.
  if (request.method !== 'GET' || !request.url.startsWith(self.location.origin)) {
    return;
  }

  const isDataRequest = request.url.includes('/data.json');

  if (isDataRequest) {
    // Network-first for live data — always try fresh, fall back to
    // whatever we last cached if the network fails.
    event.respondWith(
      fetch(request)
        .then(response => {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
          return response;
        })
        .catch(() => caches.match(request))
    );
    return;
  }

  // Cache-first for the app shell and static assets.
  event.respondWith(
    caches.match(request).then(cached => {
      if (cached) return cached;
      return fetch(request).then(response => {
        const clone = response.clone();
        caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
        return response;
      }).catch(() => {
        // Offline and not cached — for navigations, fall back to the
        // cached homepage rather than a browser error page.
        if (request.mode === 'navigate') {
          return caches.match('/');
        }
      });
    })
  );
});
