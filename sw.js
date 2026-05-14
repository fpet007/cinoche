/**
 * sw.js — Service Worker Cinoche FR v3
 * =====================================
 * Trois caches distincts avec stratégies différentes :
 *  • app-shell  : cache-first  → HTML/CSS/JS de la coquille de l'app (revalidation manuelle au déploiement)
 *  • images     : cache-first  → posters et backdrops TMDB (long-terme, peuvent grossir)
 *  • api-data   : network-first → fiches TMDB, JSON Plex (toujours préférer le frais, fallback cache offline)
 *
 * Tactique : on n'intercepte JAMAIS YouTube, Discord, Google Fonts (CDN gèrent leurs propres caches).
 */

const VERSION   = 'v3.0.0';
const APP_CACHE = `cinoche-app-${VERSION}`;
const IMG_CACHE = `cinoche-img-${VERSION}`;
const API_CACHE = `cinoche-api-${VERSION}`;

// Coquille de l'app à pré-cacher au moment de l'install
const APP_SHELL = [
  '/',
  '/index.html',
  '/manifest.webmanifest',
];

// ---- Install : pré-cache la coquille ----
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(APP_CACHE).then(cache => cache.addAll(APP_SHELL).catch(() => {
      // Si une URL de l'app shell est inaccessible (ex: 404 manifest), on ne bloque pas l'install
      return Promise.resolve();
    }))
  );
  self.skipWaiting();
});

// ---- Activate : purge des anciennes versions ----
self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys
      .filter(k => k.startsWith('cinoche-') && !k.endsWith(VERSION))
      .map(k => caches.delete(k))
    );
    await self.clients.claim();
  })());
});

// ---- Fetch : routage selon type de requête ----
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // 1. Images TMDB → cache-first (long-terme)
  if (url.hostname === 'image.tmdb.org') {
    event.respondWith(cacheFirst(req, IMG_CACHE));
    return;
  }

  // 2. API TMDB ou JSON Plex → network-first (toujours frais, fallback cache)
  if (url.hostname === 'api.themoviedb.org' || url.pathname.endsWith('plex-upcoming.json')) {
    event.respondWith(networkFirst(req, API_CACHE));
    return;
  }

  // 3. Coquille de l'app (HTML/JS/CSS du même origin) → stale-while-revalidate
  if (url.origin === self.location.origin) {
    event.respondWith(staleWhileRevalidate(req, APP_CACHE));
    return;
  }

  // 4. Le reste (YouTube facade thumbs, fonts Google, Discord, etc.) → laisse passer
});

// ============ STRATÉGIES ============

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  if (cached) return cached;
  try {
    const res = await fetch(req);
    if (res.ok) cache.put(req, res.clone());
    return res;
  } catch(e) {
    // Pas de connexion + pas en cache : retourne une réponse vide gracieuse
    return new Response('', { status: 504, statusText: 'Offline' });
  }
}

async function networkFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  try {
    const res = await fetch(req);
    if (res.ok) cache.put(req, res.clone());
    return res;
  } catch(e) {
    const cached = await cache.match(req);
    if (cached) return cached;
    return new Response(JSON.stringify({ error: 'offline' }), {
      status: 504,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

async function staleWhileRevalidate(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  const networkPromise = fetch(req).then(res => {
    if (res.ok) cache.put(req, res.clone());
    return res;
  }).catch(() => null);
  return cached || networkPromise || new Response('', { status: 504 });
}
