/**
 * updateVOD.js — v8.4 (ultimate + BingeBase integration)
 * =====================================================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle, avec un focus
 * blockbusters internationaux + films français + blockbusters VFQ.
 *
 * 🆕 NOUVEAUTÉS v8.4 :
 * ─────────────────────────────────────────────────────────────────────────────
 * ✅ Intégration complète de BingeBase (bingebase.com/releases/digital/)
 * - Génération dynamique de l'URL cible (ex: june-2026) selon le mois en cours.
 * - Scraper HTML séquentiel et robuste avec cache dédié (TTL 12h).
 * - Triangulation et enrichissement du pipeline principal (Phase 3.5).
 * - Prise en compte dans le scoring de confiance final.
 *
 * NOUVEAUTÉS HISTORIQUES (v8.2 & v8) :
 * ─────────────────────────────────────────────────────────────────────────────
 * ✅ Fenêtre FR élargie (frEnd = monthStart - 85j) pour ne rater aucune fin de mois.
 * ✅ Délais VOD par studio (Disney ~85j, Universal ~35j, Warner ~55j, etc.).
 * ✅ Système de tiers (blockbuster / mid / niche).
 * ✅ Filtre anti-production québécoise locale de faible envergure.
 * ✅ Couche AlloCiné (Triangulation FR) & Scoring de confiance adaptatif.
 *
 * Usage  : node updateVOD.js [--dry-run] [--verbose]
 * Cron   : 0 2 * * *
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════════

const TMDB_API_KEY      = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH         = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH        = path.join(__dirname, '../data/.tmdb-cache.json');
const MAXBLIZZ_CACHE    = path.join(__dirname, '../data/.maxblizz-cache.json');
const BINGEBASE_CACHE   = path.join(__dirname, '../data/.bingebase-cache.json');
const ALLOCINE_CACHE    = path.join(__dirname, '../data/.allocine-cache.json');
const OVERRIDES_PATH    = path.join(__dirname, '../data/overrides.json');

// CLI flags
const ARGS = new Set(process.argv.slice(2));
const DRY_RUN = ARGS.has('--dry-run');
const VERBOSE = ARGS.has('--verbose');

// Délais VOD génériques (fallback)
const DELAYS = {
  FRENCH  : 120,   // Chronologie des médias FR — VOD à l'acte (4 mois)
  AMERICAN:  45,   // PVOD/TVOD international standard
};

// Cache TTLs
const CACHE_TTL_HOURS     = 24;
const MAXBLIZZ_TTL_HOURS  = 12;
const BINGEBASE_TTL_HOURS = 12;
const ALLOCINE_TTL_HOURS  = 8;   // FR : peut bouger plus souvent
const API_DELAY_MS        = 130;
const MAX_PAGES_PER_ENDPOINT = 6;
const MIN_RUNTIME         = 40;

// ─── Délais par studio (TMDB production_companies.id → jours) ─────────────────
const STUDIO_VOD_DELAYS = {
  2     : 85,  // Walt Disney Pictures
  3     : 85,  // Pixar
  420   : 85,  // Marvel Studios
  7521  : 85,  // Lucasfilm
  127928: 75,  // 20th Century Studios
  43924 : 75,  // Searchlight Pictures
  10342 : 80,  // Walt Disney Animation Studios
  174   : 55,  // Warner Bros
  9993  : 55,  // DC Studios / DC Entertainment
  17    : 55,  // New Line Cinema
  1645  : 55,  // Warner Animation Group
  33    : 35,  // Universal Pictures
  6704  : 35,  // Illumination
  10146 : 60,  // Focus Features (filiale Universal)
  21887 : 35,  // DreamWorks Animation (sous Universal)
  5     : 45,  // Columbia Pictures
  34    : 45,  // Sony Pictures
  2251  : 45,  // Sony Pictures Animation
  77973 : 45,  // Sony Pictures Releasing
  4     : 50,  // Paramount Pictures
  333   : 50,  // Orion Pictures
  1632  : 45,
  35    : 45,  // Lionsgate Films
  21    : 55,  // MGM
  41077 : 75,  // A24
  61    : 60,  // Miramax
  491   : 60,  // United Artists
  194232: 30,  // Apple Studios
  20580 : 30,  // Amazon Studios / MGM Amazon
};

const TIER_THRESHOLDS = {
  BLOCKBUSTER_SCORE: 7,   // ≥ 7 points = blockbuster
  MID_SCORE        : 4,   // 4-6 points = mid-tier
};

const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]);

const SPECTACLE_KEYWORDS = [
  'symphony', 'symphonie', 'philharmonic', 'philharmonique',
  ' opera', 'opera:', "l'opéra", 'opéra de paris', 'paris opera',
  'in concert', 'live at', 'live in', 'live from', 'en concert',
  ' tour ', 'world tour', 'la tournée',
  ' ballet', 'casse-noisette', 'nutcracker', 'der nussknacker',
  'récital', 'recital', 'metropolitan opera', 'royal opera',
  'gaming x symphony',
  'théâtre', 'theatre', 'pièce de', 'comédie française', 'captation',
  'molière', 'charbon dans les veines'
];

const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

const DEFAULT_OVERRIDES = {
  'project hail mary': { date: '2026-05-12', reason: 'film à fort potentiel, repoussé' },
};

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS DE BASE
// ═══════════════════════════════════════════════════════════════════════════════

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...args) => console.log(...args);
const vlog = (...args) => { if (VERBOSE) console.log(...args); };

async function fetchWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (res.status === 429) { await sleep(2000 * attempt); continue; }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(attempt * 600);
    }
  }
}

async function fetchTextWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; CinocheFR-Bot/8.4; +https://cinochefr.space)',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        },
      });
      if (res.status === 429) { await sleep(2000 * attempt); continue; }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(attempt * 600);
    }
  }
}

async function fetchAllPages(baseUrl, maxPages = MAX_PAGES_PER_ENDPOINT) {
  const movies = [];
  for (let page = 1; page <= maxPages; page++) {
    const data = await fetchWithRetry(`${baseUrl}&page=${page}`);
    if (!data?.results?.length) break;
    movies.push(...data.results);
    if (page >= (data.total_pages || 1)) break;
    await sleep(API_DELAY_MS);
  }
  return movies;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CACHES
// ═══════════════════════════════════════════════════════════════════════════════

function loadJsonSafe(filepath, fallback = {}) {
  try { return JSON.parse(fs.readFileSync(filepath, 'utf8')); }
  catch { return fallback; }
}

function saveJsonAtomic(filepath, data) {
  try {
    fs.mkdirSync(path.dirname(filepath), { recursive: true });
    const tmp = filepath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(data), 'utf8');
    fs.renameSync(tmp, filepath);
    return true;
  } catch (err) {
    console.warn(`  ⚠️  Sauvegarde de ${path.basename(filepath)} échouée : ${err.message}`);
    return false;
  }
}

const loadCache             = () => loadJsonSafe(CACHE_PATH, {});
const saveCache             = (c) => saveJsonAtomic(CACHE_PATH, c);
const loadMaxblizzCache     = () => loadJsonSafe(MAXBLIZZ_CACHE, {});
const saveMaxblizzCache     = (c) => saveJsonAtomic(MAXBLIZZ_CACHE, c);
const loadBingebaseCache    = () => loadJsonSafe(BINGEBASE_CACHE, {});
const saveBingebaseCache   = (c) => saveJsonAtomic(BINGEBASE_CACHE, c);
const loadAllocineCache     = () => loadJsonSafe(ALLOCINE_CACHE, {});
const saveAllocineCache     = (c) => saveJsonAtomic(ALLOCINE_CACHE, c);

function isCacheEntryFresh(entry, ttlHours = CACHE_TTL_HOURS) {
  if (!entry?._cachedAt) return false;
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < ttlHours;
}

function loadOverrides() {
  const fileOverrides = loadJsonSafe(OVERRIDES_PATH, {});
  const normalizedFile = {};
  for (const [key, val] of Object.entries(fileOverrides)) {
    normalizedFile[normalizeTitle(key)] = val;
  }
  return { ...DEFAULT_OVERRIDES, ...normalizedFile };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS MÉTIER
// ═══════════════════════════════════════════════════════════════════════════════

function isFrenchProduction(details) {
  const hasFRCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin  = details.origin_country?.includes('FR') ?? false;
  const isFrLang     = details.original_language === 'fr';
  const score = (hasFRCountry ? 1 : 0) + (hasFROrigin ? 1 : 0) + (isFrLang ? 1 : 0);
  return score >= 2;
}

function hasExcludedGenre(details) {
  return details.genres?.some((g) => EXCLUDED_GENRE_IDS.has(g.id)) ?? false;
}

function isSpectacle(details) {
  const titles = [details.title, details.original_title]
    .filter(Boolean)
    .map((t) => t.toLowerCase());
  return titles.some((t) => SPECTACLE_KEYWORDS.some((kw) => t.includes(kw)));
}

function isTelefilmByTitle(details) {
  return TELEFILM_TITLE_PATTERNS.some((rx) => rx.test(details.title || ''));
}

function isGhostEntry(details) {
  const noGenres = !details.genres || details.genres.length === 0;
  const noVotes  = (details.vote_count ?? 0) === 0 && (details.vote_average ?? 0) === 0;
  const noPoster = !details.poster_path;
  const badTitle = /^untitled$|^sans titre$|^\s*$/i.test(details.title || '');
  const score = (noGenres ? 1 : 0) + (noVotes ? 1 : 0) + (noPoster ? 1 : 0) + (badTitle ? 2 : 0);
  return score >= 2;
}

function isTooShort(details) {
  const runtime = details.runtime;
  if (!runtime || runtime === 0) return false;
  return runtime < MIN_RUNTIME;
}

function hasTheatricalReleaseFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return false;
  return fr.release_dates.some((rd) => rd.type === 3 || rd.type === 2);
}

function getTheatricalDateFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const theatrical = fr.release_dates
    .filter((rd) => rd.type === 3 || rd.type === 2)
    .map((rd) => new Date(rd.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return theatrical ?? null;
}

function getOfficialDigitalDate(releaseDates) {
  for (const region of ['FR', 'US']) {
    const entry = releaseDates?.results?.find((r) => r.iso_3166_1 === region);
    if (!entry) continue;
    const digital = entry.release_dates
      .filter((rd) => rd.type === 4)
      .map((rd) => new Date(rd.release_date))
      .filter((d) => !isNaN(d))
      .sort((a, b) => a - b)[0];
    if (digital) return { date: digital, region };
  }
  return null;
}

function isQuebecLocalProduction(details) {
  const countries = details.production_countries?.map((c) => c.iso_3166_1) || [];
  const isCanadian   = countries.includes('CA');
  if (!isCanadian) return false;

  const isFrenchLang = details.original_language === 'fr';
  if (!isFrenchLang) return false;

  const hasIntlCoprod = countries.some((c) => ['FR', 'US', 'GB', 'BE', 'DE', 'IT', 'ES'].includes(c));
  if (hasIntlCoprod) return false;

  const lowBudget   = (details.budget ?? 0) < 5_000_000;
  const lowReach    = (details.popularity ?? 0) < 8;
  const fewVotes    = (details.vote_count ?? 0) < 100;

  return lowBudget && (lowReach || fewVotes);
}

function classifyTier(details) {
  const budget     = details.budget     ?? 0;
  const popularity = details.popularity ?? 0;
  const voteCount  = details.vote_count ?? 0;
  const revenue    = details.revenue    ?? 0;

  const budgetScore =
    budget >= 80_000_000 ? 3 :
    budget >= 30_000_000 ? 2 :
    budget >= 10_000_000 ? 1 : 0;

  const popScore =
    popularity >= 100 ? 3 :
    popularity >= 30  ? 2 :
    popularity >= 10  ? 1 : 0;

  const voteScore =
    voteCount >= 2000 ? 3 :
    voteCount >= 500  ? 2 :
    voteCount >= 100  ? 1 : 0;

  const revScore =
    revenue >= 200_000_000 ? 2 :
    revenue >= 50_000_000  ? 1 : 0;

  const score = budgetScore + popScore + voteScore + revScore;

  let tier;
  if (score >= TIER_THRESHOLD.BLOCKBUSTER_SCORE) tier = 'blockbuster';
  else if (score >= TIER_THRESHOLDS.MID_SCORE)    tier = 'mid';
  else                                            tier = 'niche';

  return { tier, score, breakdown: { budgetScore, popScore, voteScore, revScore } };
}

function predictVODDate(cinemaDate, details, isFrench) {
  if (isFrench) {
    const vod = new Date(cinemaDate);
    vod.setDate(vod.getDate() + DELAYS.FRENCH);
    return { date: vod, delay: DELAYS.FRENCH, method: 'fr-chronologie' };
  }

  const studios = details.production_companies || [];
  const studioDelays = studios
    .map((s) => ({ id: s.id, name: s.name, delay: STUDIO_VOD_DELAYS[s.id] }))
    .filter((s) => s.delay !== undefined);

  if (studioDelays.length > 0) {
    const lead = studioDelays.sort((a, b) => b.delay - a.delay)[0];
    const vod = new Date(cinemaDate);
    vod.setDate(vod.getDate() + lead.delay);
    return {
      date     : vod,
      delay    : lead.delay,
      method   : 'studio-mapped',
      leadStudio: lead.name,
    };
  }

  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + DELAYS.AMERICAN);
  return { date: vod, delay: DELAYS.AMERICAN, method: 'us-generic' };
}

function computeConfidence({ source, crossConfirmedBy }) {
  const sources = [source];
  if (crossConfirmedBy && crossConfirmedBy.length) {
    sources.push(...crossConfirmedBy);
  }
  const set = new Set(sources);

  if (source === 'override-manuel') {
    return { level: 'override', score: 1.0, sources };
  }

  if (source === 'officielle-fr') {
    return {
      level: set.has('allocine') ? 'very-high' : 'high',
      score: set.has('allocine') ? 0.98 : 0.95,
      sources,
    };
  }

  if (source === 'officielle-us') {
    if (set.has('allocine')) return { level: 'very-high', score: 0.93, sources };
    if (set.has('maxblizz') || set.has('bingebase')) return { level: 'high', score: 0.88, sources };
    return { level: 'high', score: 0.82, sources };
  }

  if (source === 'maxblizz' || source === 'bingebase') {
    if (set.has('allocine'))      return { level: 'very-high', score: 0.92, sources };
    if (set.has('studio-mapped')) return { level: 'high',      score: 0.78, sources };
    return { level: 'medium', score: 0.70, sources };
  }

  if (source === 'allocine') {
    return { level: 'high', score: 0.80, sources };
  }

  if (source === 'studio-mapped') {
    if (set.has('allocine')) return { level: 'medium', score: 0.75, sources };
    if (set.has('maxblizz') || set.has('bingebase')) return { level: 'medium', score: 0.72, sources };
    return { level: 'medium', score: 0.62, sources };
  }

  return { level: 'low', score: 0.45, sources };
}

function normalizeTitle(s) {
  if (!s) return '';
  return s
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/['']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/^the |^le |^la |^les |^l /, '');
}

function formatDateFR(date) {
  return date.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' });
}

function ymd(date) {
  return date.toISOString().split('T')[0];
}

// ═══════════════════════════════════════════════════════════════════════════════
// FENÊTRES & ENDPOINTS TMDB
// ═══════════════════════════════════════════════════════════════════════════════

function computeTargetWindow(now = new Date()) {
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
  return { monthStart, monthEnd, windowEnd: monthEnd };
}

function buildScanEndpoints(monthStart, windowEnd) {
  const frStart = new Date(windowEnd);
  frStart.setMonth(frStart.getMonth() - 6); frStart.setDate(frStart.getDate() - 15);
  const frEnd   = new Date(monthStart); frEnd.setDate(frEnd.getDate() - 85);

  const intlStart = new Date(windowEnd);
  intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd   = new Date(monthStart); intlEnd.setDate(intlEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false`;
  return [
    { name: 'FR/origin/popular',   url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/quality',   url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=vote_average.desc&vote_count.gte=10` },
    { name: 'FR/lang+region',      url: `${base}&with_original_language=fr&region=FR&with_release_type=2|3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/recent',     url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },
    { name: 'FR/theatrical-net',   url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/popular',        url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/quality',        url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=vote_average.desc&vote_count.gte=40` },
    { name: 'INTL/fresh',          url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=primary_release_date.desc` },
  ];
}

// ═══════════════════════════════════════════════════════════════════════════════
// DÉTAILS FILM (TMDB)
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isCacheEntryFresh(cache[key])) {
    return cache[key].data;
  }
  const url = `https://api.themoviedb.org/3/movie/${movieId}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`;
  const data = await fetchWithRetry(url);
  cache[key] = { _cachedAt: Date.now(), data };
  return data;
}

async function tmdbSearchByTitle(title, year = null) {
  const tokens = title.split(/\s+/).filter(Boolean);
  const candidates = [title];
  if (tokens.length >= 3 && tokens[0].length >= 3) candidates.push(tokens.slice(1).join(' '));
  if (tokens.length >= 4 && tokens[1].length >= 3) candidates.push(tokens.slice(2).join(' '));

  for (const query of candidates) {
    let url = `https://api.themoviedb.org/3/search/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false&query=${encodeURIComponent(query)}`;
    if (year) url += `&year=${year}`;
    let data;
    try { data = await fetchWithRetry(url); } catch { continue; }

    const results = data?.results || [];
    if (!results.length) continue;

    const ranked = results
      .filter((r) => {
        const y = r.release_date ? parseInt(r.release_date.slice(0, 4), 10) : 0;
        return y >= 2024 && (r.vote_count ?? 0) >= 1;
      })
      .sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0));

    if (ranked.length > 0) return ranked[0];
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MODULE MAXBLIZZ
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchMaxblizzReleases() {
  const PARSER_VERSION = 2;
  const cache = loadMaxblizzCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const articleCache = cacheValid ? (cache.articles || {}) : {};
  const listFresh = cacheValid && cache._listCachedAt
    && (Date.now() - cache._listCachedAt) / 3600000 < MAXBLIZZ_TTL_HOURS;

  if (listFresh && Array.isArray(cache.releases)) {
    log(`  💾 MaxBlizz : cache valide (${cache.releases.length} entrées)`);
    return cache.releases.map((r) => ({ ...r, date: new Date(r.date) }));
  }

  log(`  🌐 MaxBlizz : scraping en cours...`);
  const releases = [];

  try {
    const listHtml = await fetchTextWithRetry('https://maxblizz.com/dvd-and-vod-release-dates/');
    const linkRegex = /href="(https:\/\/maxblizz\.com\/([a-z0-9-]+)-vod-release-date-revealed\/?)"/gi;
    const articles = new Map();
    let m;
    while ((m = linkRegex.exec(listHtml)) !== null) {
      if (!articles.has(m[1])) articles.set(m[1], m[2]);
    }

    const monthPattern = '(January|February|March|April|May|June|July|August|September|October|November|December)';
    const datePart = `${monthPattern}\\s+(\\d{1,2}),?\\s+(\\d{4})`;
    const newArticleCache = {};

    for (const [url, slug] of articles) {
      let articleData = articleCache[url];
      const cachedFresh = articleData && (Date.now() - articleData._cachedAt) / 3600000 < 168;

      if (!cachedFresh) {
        try {
          await sleep(250);
          const html = await fetchTextWithRetry(url, 2);
          const strongDateRegex = new RegExp(`(?:<strong>|\\*\\*)\\s*${datePart}\\s*(?:</strong>|\\*\\*)`, 'gi');
          const contextDateRegex = new RegExp(`(?:starting|available\\s+(?:on|to)|rent\\s+(?:on|it)|buy\\s+(?:on|it)|arrives?\\s+on|arriving\\s+on|release\\s+date\\s+(?:is|will\\s+be))\\s+${datePart}`, 'gi');
          const body = html.length > 3000 ? html.slice(2000) : html;
          const candidates = [];
          let dm;
          while ((dm = strongDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'strong', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          while ((dm = contextDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'context', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          const valid = candidates.filter((c) => !isNaN(c.date));
          let chosenDate = null;
          if (valid.filter(c => c.src === 'strong').length > 0) chosenDate = valid.filter(c => c.src === 'strong').sort((a,b)=>a.date-b.date)[0].date;
          else if (valid.filter(c => c.src === 'context').length > 0) chosenDate = valid.filter(c => c.src === 'context').sort((a,b)=>a.date-b.date)[0].date;
          
          articleData = { _cachedAt: Date.now(), date: chosenDate ? chosenDate.toISOString() : null, slug };
        } catch {
          articleData = { _cachedAt: Date.now(), date: null, slug };
        }
      }

      newArticleCache[url] = articleData;
      if (articleData.date) {
        releases.push({ slug, title: slug.replace(/-/g, ' '), date: new Date(articleData.date), url });
      }
    }

    saveMaxblizzCache({ _parserVersion: PARSER_VERSION, _listCachedAt: Date.now(), articles: newArticleCache, releases: releases.map(r => ({...r, date: r.date.toISOString()})) });
  } catch (err) {
    console.warn(`  ⚠️  MaxBlizz scraping échoué : ${err.message}`);
  }
  return releases;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 🆕 MODULE BINGEBASE
// ═══════════════════════════════════════════════════════════════════════════════

function parseBingebaseHtml(html, fallbackYear) {
  const items = [];
  const monthMap = { january:0, february:1, march:2, april:3, may:4, june:5, july:6, august:7, september:8, october:9, november:10, december:11 };
  
  const dateRx = /(?:##|h[23]|class="[^"]*")?\s*(?:[A-Za-z]+,?\s+)?(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\\s+(\d{4})/gi;
  const dateMatches = [];
  let match;
  
  while ((match = dateRx.exec(html)) !== null) {
    const monthStr = match[1].toLowerCase();
    const day = parseInt(match[2], 10);
    const year = parseInt(match[3], 10);
    const dateObj = new Date(Date.UTC(year, monthMap[monthStr], day, 12, 0, 0));
    if (!isNaN(dateObj)) {
      dateMatches.push({ index: match.index, length: match[0].length, date: dateObj });
    }
  }
  
  const movieRx = /(?:href="\/movies\/([a-z0-9-]+)"[^>]*>([^<]+)|###\s*(?:[\d.]+\s+)?([^\n(]+)\s*\((\d{4})\))/gi;
  
  for (let i = 0; i < dateMatches.length; i++) {
    const startIdx = dateMatches[i].index + dateMatches[i].length;
    const endIdx = (i + 1 < dateMatches.length) ? dateMatches[i+1].index : html.length;
    const sectionHtml = html.slice(startIdx, endIdx);
    
    let mMatch;
    movieRx.lastIndex = 0;
    while ((mMatch = movieRx.exec(sectionHtml)) !== null) {
      let title = '';
      let slug = '';
      if (mMatch[1]) {
        slug = mMatch[1];
        title = mMatch[2].replace(/<[^>]+>/g, '').trim();
      } else if (mMatch[3]) {
        title = mMatch[3].trim();
      }
      
      if (title && title.length > 1) {
        title = title.replace(/&amp;/g, '&').replace(/&quot;/g, '"');
        items.push({ title, date: dateMatches[i].date, slug });
      }
    }
  }
  
  if (items.length === 0) {
    const genericRx = /href="\/movies\/([a-z0-9-]+)"[^>]*>([^<]+)/gi;
    let genMatch;
    while ((genMatch = genericRx.exec(html)) !== null) {
      const title = genMatch[2].replace(/<[^>]+>/g, '').trim().replace(/&amp;/g, '&');
      items.push({ title, date: null, slug: genMatch[1] });
    }
  }
  
  return items;
}

async function fetchBingebaseReleases(targetDate = new Date()) {
  const PARSER_VERSION = 1;
  const cache = loadBingebaseCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const listFresh = cacheValid && cache._listCachedAt
    && (Date.now() - cache._listCachedAt) / 3600000 < BINGEBASE_TTL_HOURS;

  if (listFresh && Array.isArray(cache.releases)) {
    log(`  💾 BingeBase : cache valide (${cache.releases.length} entrées)`);
    return cache.releases.map((r) => ({ ...r, date: r.date ? new Date(r.date) : null }));
  }

  log(`  🌐 BingeBase : scraping en cours...`);
  const releases = [];
  
  const MONTH_NAMES_EN = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  const monthName = MONTH_NAMES_EN[targetDate.getMonth()];
  const year = targetDate.getFullYear();
  const url = `https://bingebase.com/releases/digital/${monthName}-${year}`;

  try {
    const html = await fetchTextWithRetry(url, 2);
    const items = parseBingebaseHtml(html, year);
    
    for (const it of items) {
      if (it.date) {
        releases.push({ title: it.title, date: it.date, slug: it.slug, url });
      }
    }
    
    log(`     ✓ ${releases.length} films extraits de BingeBase`);
    saveBingebaseCache({
      _parserVersion: PARSER_VERSION,
      _listCachedAt: Date.now(),
      releases: releases.map((r) => ({ ...r, date: r.date.toISOString() })),
    });
  } catch (err) {
    console.warn(`  ⚠️  BingeBase scraping échoué : ${err.message} — on continue sans.`);
  }
  return releases;
}

async function enrichWithBingebase({ finalResults, monthStart, monthEnd, cache, overrides }) {
  log('\n  📡  Enrichissement BingeBase :');
  const bbReleases = await fetchBingebaseReleases(monthStart);
  if (bbReleases.length === 0) return { added: 0, overridden: 0 };

  const existingIds       = new Set(finalResults.map((m) => m.tmdb_id));
  const existingTitles    = new Set(finalResults.map((m) => normalizeTitle(m.title)));
  const existingOriginals = new Set(finalResults.map((m) => normalizeTitle(m.original_title || '')));

  let added = 0, overridden = 0, skipped = 0, dropQc = 0, dropNiche = 0;

  for (const bb of bbReleases) {
    if (!bb.date) { skipped++; continue; }
    const slugNorm = normalizeTitle(bb.title);
    let bbDate = bb.date;
    let isOverride = false;

    const tokens = slugNorm.split(' ');
    const overrideKeys = [slugNorm];
    if (tokens.length >= 3) overrideKeys.push(tokens.slice(1).join(' '));
    if (tokens.length >= 4) overrideKeys.push(tokens.slice(2).join(' '));
    for (const k of overrideKeys) {
      if (overrides[k]) {
        bbDate = new Date(overrides[k].date);
        isOverride = true;
        log(`     ★ Override "${bb.title}" → ${formatDateFR(bbDate)} (${overrides[k].reason})`);
        break;
      }
    }

    if (bbDate < monthStart || bbDate > monthEnd) { skipped++; continue; }

    const tmdbHit = await tmdbSearchByTitle(bb.title);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) {
      vlog(`     ✗ TMDB sans match pour "${bb.title}"`);
      skipped++; continue;
    }

    if (existingIds.has(tmdbHit.id)) {
      const existing = finalResults.find((m) => m.tmdb_id === tmdbHit.id);
      existing._crossConfirmedBy = existing._crossConfirmedBy || [];
      if (!existing._crossConfirmedBy.includes('bingebase')) {
        existing._crossConfirmedBy.push('bingebase');
      }
      const oldDate = existing.plex_release;
      if (existing.plex_release !== formatDateFR(bbDate)) {
        if (existing.source === 'studio-mapped' || existing.source === 'prédite') {
          existing.plex_release = formatDateFR(bbDate);
          existing._sortDate    = bbDate.getTime();
          existing.source       = isOverride ? 'override-manuel' : 'bingebase';
          log(`     ↻ "${tmdbHit.title}" : ${oldDate} → ${formatDateFR(bbDate)} (bingebase)`);
          overridden++;
        }
      }
      continue;
    }

    const tmdbTitleNorm = normalizeTitle(tmdbHit.title);
    const tmdbOrigNorm  = normalizeTitle(tmdbHit.original_title || '');
    if (existingTitles.has(tmdbTitleNorm) || existingOriginals.has(tmdbOrigNorm)) {
      skipped++; continue;
    }

    let details;
    try {
      details = await fetchMovieDetails(tmdbHit.id, cache);
      await sleep(API_DELAY_MS);
    } catch { skipped++; continue; }

    if (isQuebecLocalProduction(details)) {
      vlog(`     🚫 "${tmdbHit.title}" filtré (prod QC locale)`);
      dropQc++; continue;
    }

    const isFrench = isFrenchProduction(details);
    const tierInfo = classifyTier(details);
    if (!isFrench && tierInfo.tier === 'niche') {
      vlog(`     🚫 "${tmdbHit.title}" filtré (niche non-FR, score=${tierInfo.score})`);
      dropNiche++; continue;
    }

    const cinemaDate = details.release_date ? new Date(details.release_date) : null;
    const studios    = details.production_companies || [];
    const leadStudio = studios.find((s) => STUDIO_VOD_DELAYS[s.id]);

    finalResults.push({
      title          : details.title || tmdbHit.title,
      plex_release   : formatDateFR(bbDate),
      tmdb_id        : tmdbHit.id,
      poster_path    : details.poster_path || tmdbHit.poster_path,
      original_title : details.original_title || tmdbHit.original_title,
      cinema_date    : cinemaDate ? formatDateFR(cinemaDate) : null,
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : details.vote_count ?? 0,
      genres         : (details.genres || []).map((g) => g.name),
      is_french      : isFrench,
      source         : isOverride ? 'override-manuel' : 'bingebase',
      _tier          : tierInfo.tier,
      _tierScore     : tierInfo.score,
      _leadStudio    : leadStudio?.name || null,
      _sortDate      : bbDate.getTime(),
      _popularity    : details.popularity ?? tmdbHit.popularity ?? 0,
      _crossConfirmedBy: [],
    });

    log(`     ✓ Ajouté via BingeBase : ${details.title || tmdbHit.title} → ${formatDateFR(bbDate)} [${tierInfo.tier}]`);
    added++;
    existingIds.add(tmdbHit.id);
  }

  log(`     📊 ${added} ajouts, ${overridden} dates corrigées, ${dropQc} QC locaux jetés, ${dropNiche} niche non-FR jetés, ${skipped} ignorés`);
  return { added, overridden };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MODULE ALLOCINE
// ═══════════════════════════════════════════════════════════════════════════════

const ALLOCINE_URLS = [
  'https://www.allocine.fr/video/aladelocation/',
  'https://www.allocine.fr/video/aladevente/',
];

const FR_MONTHS = {
  'janvier':0, 'février':1, 'fevrier':1, 'mars':2, 'avril':3, 'mai':4, 'juin':5,
  'juillet':6, 'août':7, 'aout':7, 'septembre':8, 'octobre':9, 'novembre':10, 'décembre':11, 'decembre':11,
};

function parseFrenchDate(text, fallbackYear = new Date().getFullYear()) {
  if (!text) return null;
  const cleaned = text.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/1er/g, '1');
  const rx = /(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)(?:\s+(\d{4}))?/g;
  const candidates = [];
  let m;
  while ((m = rx.exec(cleaned)) !== null) {
    const day   = parseInt(m[1], 10);
    const month = FR_MONTHS[m[2]];
    const year  = m[3] ? parseInt(m[3], 10) : fallbackYear;
    if (month === undefined || day < 1 || day > 31) continue;
    const d = new Date(Date.UTC(year, month, day, 12, 0, 0));
    if (!isNaN(d)) candidates.push(d);
  }
  return candidates.length ? candidates.sort((a, b) => a - b)[0] : null;
}

function parseAllocineHtml(html) {
  const items = [];
  const seenTitles = new Set();

  const filmCardRegex = /<a[^>]*class="[^"]*meta-title-link[^"]*"[^>]*>([^<]{2,100})<\/a>([\s\S]{0,800}?)(?=<a[^>]*class="[^"]*meta-title-link|$)/gi;
  let m;
  while ((m = filmCardRegex.exec(html)) !== null) {
    const rawTitle = m[1].trim();
    if (!rawTitle || seenTitles.has(rawTitle.toLowerCase())) continue;
    const date = parseFrenchDate(m[2]);
    if (!date) continue;
    seenTitles.add(rawTitle.toLowerCase());
    items.push({ title: rawTitle, date });
  }

  if (items.length < 3) {
    const fallbackRegex = /<a[^>]+href="\/film\/fichefilm[^"]+"[^>]*>([^<]{2,100})<\/a>([\s\S]{0,500}?)(sortie|vod|disponibilit[ée])[^<]{0,50}(\d{1,2}\s+(?:janvier|f[ée]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[ée]cembre)(?:\s+\d{4})?)/gi;
    while ((m = fallbackRegex.exec(html)) !== null) {
      const rawTitle = m[1].trim();
      if (!rawTitle || seenTitles.has(rawTitle.toLowerCase())) continue;
      const date = parseFrenchDate(m[4]);
      if (!date) continue;
      seenTitles.add(rawTitle.toLowerCase());
      items.push({ title: rawTitle, date });
    }
  }
  return items;
}

async function fetchAllocineReleases() {
  const PARSER_VERSION = 1;
  const cache = loadAllocineCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const fresh = cacheValid && cache._cachedAt && (Date.now() - cache._cachedAt) / 3600000 < ALLOCINE_TTL_HOURS;

  if (fresh && Array.isArray(cache.releases)) {
    log(`  💾 AlloCiné : cache valide (${cache.releases.length} entrées)`);
    return cache.releases.map((r) => ({ ...r, date: new Date(r.date) }));
  }

  log(`  🌐 AlloCiné : scraping en cours...`);
  const releases = [];
  const seen = new Set();

  for (const url of ALLOCINE_URLS) {
    try {
      await sleep(400);
      const html = await fetchTextWithRetry(url, 2);
      const items = parseAllocineHtml(html);
      for (const it of items) {
        const key = normalizeTitle(it.title);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        releases.push({ title: it.title, date: it.date, source_url: url });
      }
    } catch (err) {
      console.warn(`     ⚠️  ${url} échoué : ${err.message}`);
    }
  }

  saveAllocineCache({ _parserVersion: PARSER_VERSION, _cachedAt: Date.now(), releases: releases.map(r => ({ ...r, date: r.date.toISOString() })) });
  return releases;
}

async function enrichWithAllocine({ finalResults, monthStart, monthEnd, cache }) {
  log('\n  📡  Cross-référence AlloCiné (triangulation FR) :');
  const allocineReleases = await fetchAllocineReleases();
  if (allocineReleases.length === 0) return { confirmed: 0, added: 0, corrected: 0 };

  const existingByTitle = new Map();
  for (const r of finalResults) {
    existingByTitle.set(normalizeTitle(r.title), r);
    if (r.original_title) existingByTitle.set(normalizeTitle(r.original_title), r);
  }

  let confirmed = 0, added = 0, corrected = 0, skipped = 0;

  for (const ac of allocineReleases) {
    if (ac.date < monthStart || ac.date > monthEnd) { skipped++; continue; }
    const key = normalizeTitle(ac.title);
    const existing = existingByTitle.get(key);

    if (existing) {
      existing._crossConfirmedBy = existing._crossConfirmedBy || [];
      if (!existing._crossConfirmedBy.includes('allocine')) {
        existing._crossConfirmedBy.push('allocine');
        confirmed++;

        const diffDays = Math.abs(existing._sortDate - ac.date.getTime()) / 86400000;
        if (diffDays > 5 && ['studio-mapped', 'prédite', 'maxblizz', 'bingebase'].includes(existing.source)) {
          const oldDate = existing.plex_release;
          existing.plex_release = formatDateFR(ac.date);
          existing._sortDate    = ac.date.getTime();
          existing.source       = 'officielle-allocine';
          corrected++;
          log(`     ↻ "${existing.title}" : ${oldDate} → ${formatDateFR(ac.date)} (AlloCiné prime)`);
        }
      }
      continue;
    }

    const tmdbHit = await tmdbSearchByTitle(ac.title);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) { skipped++; continue; }

    let details;
    try { details = await fetchMovieDetails(tmdbHit.id, cache); await sleep(API_DELAY_MS); }
    catch { skipped++; continue; }

    if (hasExcludedGenre(details) || isTelefilmByTitle(details) || isSpectacle(details) || isQuebecLocalProduction(details)) { skipped++; continue; }

    const isFrench = isFrenchProduction(details);
    const tierInfo = classifyTier(details);
    if (!isFrench && tierInfo.tier === 'niche') { skipped++; continue; }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? (details.release_date ? new Date(details.release_date) : null);
    const studios      = details.production_companies || [];
    const leadStudio   = studios.find((s) => STUDIO_VOD_DELAYS[s.id]);

    finalResults.push({
      title          : details.title || tmdbHit.title,
      plex_release   : formatDateFR(ac.date),
      tmdb_id        : tmdbHit.id,
      poster_path    : details.poster_path || tmdbHit.poster_path,
      original_title : details.original_title || tmdbHit.original_title,
      cinema_date    : cinemaDate ? formatDateFR(cinemaDate) : null,
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : details.vote_count ?? 0,
      genres         : (details.genres || []).map((g) => g.name),
      is_french      : isFrench,
      source         : 'allocine',
      _tier          : tierInfo.tier,
      _tierScore     : tierInfo.score,
      _leadStudio    : leadStudio?.name || null,
      _sortDate      : ac.date.getTime(),
      _popularity    : details.popularity ?? 0,
      _crossConfirmedBy: [],
    });

    log(`     ➕ Ajouté via AlloCiné : ${details.title || tmdbHit.title} → ${formatDateFR(ac.date)} [${tierInfo.tier}]`);
    added++;
    existingByTitle.set(key, finalResults[finalResults.length - 1]);
  }

  log(`     📊 ${confirmed} confirmations, ${corrected} dates corrigées, ${added} ajouts, ${skipped} ignorés`);
  return { confirmed, added, corrected };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE PRINCIPAL
// ═══════════════════════════════════════════════════════════════════════════════

async function updateVOD() {
  const t0 = Date.now();
  log('🎬  updateVOD v8.4 — démarrage...');
  if (DRY_RUN) log('   ⚙️  Mode --dry-run actif : aucune écriture du JSON final');
  log('');

  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);
  log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  const cache     = loadCache();
  const overrides = loadOverrides();

  // ─── Phase 1 : Scan TMDB ─────────────────────────────────────────────────────
  const endpoints = buildScanEndpoints(monthStart, windowEnd);
  const rawMovies = [];
  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`  🔎  [${String(i + 1).padStart(2, '0')}/${endpoints.length}] ${name.padEnd(22)} `);
    try {
      const results = await fetchAllPages(url);
      rawMovies.push(...results);
      log(`→ ${results.length} films`);
    } catch (err) { log(`⚠️  ${err.message}`); }
    await sleep(API_DELAY_MS);
  }

  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  log(`\n  📦  ${uniqueMovies.length} films uniques\n`);

  // ─── Phase 2 : Analyse détaillée + filtres ───────────────────────────────────
  const finalResults = [];
  const dropReasons = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0,
    spectacle: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, beforeWindow: 0, afterWindow: 0, delayTooShort: 0,
    quebecLocal: 0, nicheNonFrench: 0,
  };
  let cacheHits = 0;

  log('  🔬  Analyse détaillée :');
  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    else await sleep(API_DELAY_MS);

    let details;
    try { details = await fetchMovieDetails(movie.id, cache); }
    catch { dropReasons.noDetails++; continue; }

    if (!details.release_date)      { dropReasons.noReleaseDate++; continue; }
    if (hasExcludedGenre(details))  { dropReasons.excludedGenre++; continue; }
    if (isTelefilmByTitle(details)) { dropReasons.telefilmByTitle++; continue; }
    if (isSpectacle(details))       { dropReasons.spectacle++; continue; }
    if (isTooShort(details))        { dropReasons.tooShort++; continue; }
    if (isGhostEntry(details))      { dropReasons.ghostEntry++; continue; }
    if (!details.genres || details.genres.length === 0) { dropReasons.noGenresAtAll++; continue; }
    if (isQuebecLocalProduction(details)) { dropReasons.quebecLocal++; continue; }

    const isFrench  = isFrenchProduction(details);
    const minDelay  = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    if (!hasTheatricalReleaseFR(details.release_dates)) { dropReasons.noFRTheatrical++; continue; }

    const voteCount  = details.vote_count ?? 0;
    const popularity = movie.popularity ?? details.popularity ?? 0;
    if (voteCount < 5 && popularity < 1.5) { dropReasons.lowQuality++; continue; }

    const tierInfo = classifyTier(details);
    if (!isFrench && tierInfo.tier === 'niche') { dropReasons.nicheNonFrench++; continue; }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    const officialDigital = getOfficialDigitalDate(details.release_dates);
    const predicted       = predictVODDate(cinemaDate, details, isFrench);

    let vodDate, source, leadStudio;
    if (officialDigital) {
      vodDate    = officialDigital.date;
      source     = `officielle-${officialDigital.region.toLowerCase()}`;
      leadStudio = predicted.leadStudio || null;
    } else if (predicted.method === 'studio-mapped') {
      vodDate    = predicted.date;
      source     = 'studio-mapped';
      leadStudio = predicted.leadStudio;
    } else {
      vodDate    = predicted.date;
      source     = 'prédite';
      leadStudio = null;
    }

    const titleNorm = normalizeTitle(details.title || movie.title);
    const origNorm  = normalizeTitle(details.original_title || '');
    const overrideMatch = overrides[titleNorm] || overrides[origNorm];
    if (overrideMatch) {
      const overrideDate = new Date(overrideMatch.date);
      if (!isNaN(overrideDate)) {
        log(`     ★ Override "${details.title || movie.title}" → ${formatDateFR(overrideDate)} (${overrideMatch.reason})`);
        vodDate = overrideDate;
        source  = 'override-manuel';
      }
    }

    if ((vodDate - cinemaDate) / 86400000 < minDelay && !officialDigital) { dropReasons.delayTooShort++; continue; }
    if (vodDate < monthStart) { dropReasons.beforeWindow++; continue; }
    if (vodDate > windowEnd)  { dropReasons.afterWindow++;  continue; }

    finalResults.push({
      title          : details.title || movie.title,
      plex_release   : formatDateFR(vodDate),
      tmdb_id        : movie.id,
      poster_path    : details.poster_path || movie.poster_path,
      original_title : details.original_title,
      cinema_date    : formatDateFR(cinemaDate),
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : voteCount,
      genres         : details.genres.map((g) => g.name),
      is_french      : isFrench,
      source,
      _tier          : tierInfo.tier,
      _tierScore     : tierInfo.score,
      _leadStudio    : leadStudio,
      _predictedDelay: predicted.delay,
      _sortDate      : vodDate.getTime(),
      _popularity    : popularity,
      _crossConfirmedBy: [],
    });

    const flag     = isFrench ? '🇫🇷' : '🌍';
    const srcShort = source.startsWith('officielle') ? '✓' : (source === 'studio-mapped' ? '◎' : '~');
    const tierTag  = tierInfo.tier === 'blockbuster' ? '★' : tierInfo.tier === 'mid' ? '·' : ' ';
    log(`     ${flag} ${srcShort}${tierTag} ${(details.title || '').padEnd(42).slice(0, 42)} → ${formatDateFR(vodDate)}`);
  }

  // ─── Phase 3 : Enrichissement MaxBlizz ───────────────────────────────────────
  await enrichWithMaxblizz({ finalResults, monthStart, monthEnd, cache, overrides });

  // ─── Phase 3.5 : Enrichissement BingeBase ────────────────────────────────────
  await enrichWithBingebase({ finalResults, monthStart, monthEnd, cache, overrides });

  // ─── Phase 4 : Triangulation AlloCiné ────────────────────────────────────────
  await enrichWithAllocine({ finalResults, monthStart, monthEnd, cache });

  // ─── Phase 5 : Scoring de confiance final ────────────────────────────────────
  log('\n  🎯  Calcul des scores de confiance :');
  for (const item of finalResults) {
    item.confidence = computeConfidence({ source: item.source, crossConfirmedBy: item._crossConfirmedBy || [] });
  }

  // ─── Phase 6 : Tri, dédup, sérialisation ─────────────────────────────────────
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });

  const deduped = Array.from(new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values());
  const output = deduped.map((m) => {
    const clean = {};
    for (const [k, v] of Object.entries(m)) {
      if (k.startsWith('_')) continue;
      clean[k] = v;
    }
    clean.tier        = m._tier;
    clean.lead_studio = m._leadStudio;
    return clean;
  });

  if (!DRY_RUN) {
    fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
    const tmpPath = DATA_PATH + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
    fs.renameSync(tmpPath, DATA_PATH);
  }

  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) { if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k]; }
  saveCache(cache);

  // Récap détaillé
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  const frCount    = output.filter((m) => m.is_french).length;
  const intlCount  = output.length - frCount;
  const byTier     = output.reduce((acc, m) => { acc[m.tier] = (acc[m.tier] || 0) + 1; return acc; }, {});
  const bySource   = output.reduce((acc, m) => { acc[m.source] = (acc[m.source] || 0) + 1; return acc; }, {});
  const byConf     = output.reduce((acc, m) => { acc[m.confidence.level] = (acc[m.confidence.level] || 0) + 1; return acc; }, {});
  const avgConf    = output.length ? (output.reduce((s, m) => s + m.confidence.score, 0) / output.length).toFixed(2) : 'N/A';

  log('\n  ═══════════════════════════════════════════════════════════════════════');
  log(`  ✅ Terminé en ${elapsed}s — ${output.length} films générés`);
  log(`     🇫🇷 Français : ${frCount}    🌍 International : ${intlCount}`);
  log('  ─────────────────────────────────────────────────────────────────────');
  log(`     Par tier      : ${Object.entries(byTier).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Par source    : ${Object.entries(bySource).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Par confiance : ${Object.entries(byConf).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Confiance moyenne : ${avgConf} / 1.00`);
  log('  ═══════════════════════════════════════════════════════════════════════');

  if (DRY_RUN) log('\n  ⚙️  Mode dry-run : le fichier final N\'a PAS été écrit.');
  else log(`\n  💾  Fichier écrit : ${DATA_PATH}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
