// sw.js — London GP Directory service worker
//
// Required for PWA installability (and therefore for TWA/App-store
// wrapping). Strategy:
//   - /data.json (the live practice data): network-first, falling back to
//     cache when offline.
//   - HTML pages and every other same-origin GET: network-first, falling
//     back to cache (and, for navigations, to the cached homepage) when
//     offline. This means a fresh deploy is always picked up on the next
//     visit — the SW never traps visitors on a stale cached page.
//   - A small static shell is precached so the app still opens offline.
//
// NOTE: bump CACHE_NAME on any change that must invalidate old caches. The
// activate handler deletes every cache whose name != CACHE_NAME, so bumping
// the version purges stale entries for existing visitors.

const CACHE_NAME = 'londongp-v2';
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

  // Network-first for everything (live data, HTML pages, static assets),
  // with a cache fallback so the app still works offline. Successful
  // responses are cached for that offline fallback.
  event.respondWith(
    fetch(request)
      .then(response => {
        if (response && response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
        }
        return response;
      })
      .catch(() =>
        caches.match(request).then(cached => {
          if (cached) return cached;
          // Offline and not cached — for navigations, fall back to the
          // cached homepage rather than a browser error page.
          if (request.mode === 'navigate') {
            return caches.match('/');
          }
        })
      )
  );
});
