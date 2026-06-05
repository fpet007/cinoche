/**
 * updateVOD.js — v8.4 (BingeBase intégré)
 * =====================================================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle, avec un focus
 * blockbusters internationaux + films français + blockbusters VFQ.
 *
 * 🆕 NOUVEAUTÉS v8.4 :
 * ─────────────────────────────────────────────────────────────────────────────
 * ✅ Module BingeBase intégré (4e source de triangulation US).
 *    URL dynamique : https://bingebase.com/releases/digital/[mois]-[année]
 *    Scrape les sections ## [Date] + films ### [rating] [Title] ([year]).
 *    Pipeline : Phase 3.5 entre MaxBlizz (US) et AlloCiné (FR).
 *    Rôle : cross-confirmer les dates US + ajouter les films manquants (avec
 *    filtre tier + QC + filtre genre — mêmes règles que les autres sources).
 *    Cache dédié .bingebase-cache.json avec TTL 12h.
 *
 * 🔒 v8.3 (rappel) :
 * ─────────────────────────────────────────────────────────────────────────────
 * ✅ Fenêtre FR de scan élargie : frEnd = monthStart - 85j (au lieu de -100j).
 *    Cause : un film sorti fin janvier (ex: Gourou le 28/01) a sa VOD à 120j,
 *    soit le 28 mai, qui tombe dans le mois cible. Avec frEnd à -100j,
 *    le scan s'arrêtait au 21 janvier et ratait toutes les sorties de fin
 *    janvier dont la VOD tombe dans le mois en cours.
 *
 * 🔒 Toute la logique v8 est préservée (studios, tiers, AlloCiné, MaxBlizz,
 *    anti-QC, confiance, etc.). Aucun changement de format JSON de sortie.
 *
 * NOUVEAUTÉS v8 (rappel) :
 * ─────────────────────────────────────────────────────────────────────────────
 * ✅ Délais VOD par studio (mapping TMDB) :
 *      Disney/Marvel/Pixar  ~85j  |  Universal  ~35j  |  Warner ~55j
 *      Sony ~45j  |  Paramount ~50j  |  Lionsgate ~45j  |  A24 ~75j
 *      Le délai US générique 45j reste en fallback pour studios inconnus.
 *
 * ✅ Système de tiers (blockbuster / mid / niche)
 * ✅ Filtre anti-production québécoise locale
 * ✅ Couche AlloCiné — Triangulation FR
 * ✅ Scoring de confiance par film
 * ✅ Overrides externes (data/overrides.json)
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
const ALLOCINE_CACHE    = path.join(__dirname, '../data/.allocine-cache.json');
const BINGEBASE_CACHE   = path.join(__dirname, '../data/.bingebase-cache.json');
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
const ALLOCINE_TTL_HOURS  = 8;   // FR : peut bouger plus souvent
const BINGEBASE_TTL_HOURS = 12;  // 🆕 v8.4 BingeBase (US : stable dans la journée)
const API_DELAY_MS        = 130;
const MAX_PAGES_PER_ENDPOINT = 6;
const MIN_RUNTIME         = 40;

// ─── Délais par studio (TMDB production_companies.id → jours) ─────────────────
// Sources : observation historique des fenêtres PVOD US 2022-2025 par studio.
// On prend toujours le MAX parmi les studios du film (le "lead" dicte la fenêtre).
const STUDIO_VOD_DELAYS = {
  // Disney empire — hold long pour pousser vers Disney+
  2     : 85,  // Walt Disney Pictures
  3     : 85,  // Pixar
  420   : 85,  // Marvel Studios
  7521  : 85,  // Lucasfilm
  127928: 75,  // 20th Century Studios
  43924 : 75,  // Searchlight Pictures
  10342 : 80,  // Walt Disney Animation Studios
  // Warner Bros — fenêtre moyenne
  174   : 55,  // Warner Bros
  9993  : 55,  // DC Studios / DC Entertainment
  17    : 55,  // New Line Cinema
  1645  : 55,  // Warner Animation Group
  // Universal — fenêtre courte (deal AMC/Cinemark depuis 2020)
  33    : 35,  // Universal Pictures
  6704  : 35,  // Illumination
  10146 : 60,  // Focus Features (filiale Universal, fenêtre plus longue)
  21887 : 35,  // DreamWorks Animation (sous Universal)
  // Sony / Columbia
  5     : 45,  // Columbia Pictures
  34    : 45,  // Sony Pictures
  2251  : 45,  // Sony Pictures Animation
  77973 : 45,  // Sony Pictures Releasing
  // Paramount
  4     : 50,  // Paramount Pictures
  333   : 50,  // Orion Pictures
  // Lionsgate
  1632  : 45,
  35    : 45,  // Lionsgate Films
  // MGM (Amazon)
  21    : 55,
  // Indés premium (souvent hold long avant streaming)
  41077 : 75,  // A24
  61    : 60,  // Miramax
  491   : 60,  // United Artists
  // Apple / Amazon (théâtrales rares mais quand ça arrive : court)
  194232: 30,  // Apple Studios
  20580 : 30,  // Amazon Studios / MGM Amazon
};

// Seuils du système de tiers (scoring composite)
const TIER_THRESHOLDS = {
  BLOCKBUSTER_SCORE: 7,   // ≥ 7 points = blockbuster
  MID_SCORE        : 4,   // 4-6 points = mid-tier (A24, prestige, prestige indés)
                          // < 4 points = niche (jetable côté international)
};

// Genres TMDB exclus : Documentaire (99), Musique (10402), Téléfilm (10770)
const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]);

// Mots-clés titres : captation spectacle vivant / théâtre
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

// Téléfilms policiers régionaux FR
const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

// Overrides par défaut (codés en dur, hérités v7). Le fichier externe peut les compléter.
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
          'User-Agent': 'Mozilla/5.0 (compatible; CinocheFR-Bot/8.0; +https://cinochefr.space)',
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
// CACHES (TMDB / MaxBlizz / AlloCiné)
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
const loadAllocineCache     = () => loadJsonSafe(ALLOCINE_CACHE, {});
const saveAllocineCache     = (c) => saveJsonAtomic(ALLOCINE_CACHE, c);
const loadBingebaseCache    = () => loadJsonSafe(BINGEBASE_CACHE, {});  // 🆕 v8.4
const saveBingebaseCache    = (c) => saveJsonAtomic(BINGEBASE_CACHE, c); // 🆕 v8.4

function isCacheEntryFresh(entry, ttlHours = CACHE_TTL_HOURS) {
  if (!entry?._cachedAt) return false;
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < ttlHours;
}

function loadOverrides() {
  const fileOverrides = loadJsonSafe(OVERRIDES_PATH, {});
  // Normalise les clés du fichier pour matcher le lookup (qui utilise normalizeTitle)
  const normalizedFile = {};
  for (const [key, val] of Object.entries(fileOverrides)) {
    normalizedFile[normalizeTitle(key)] = val;
  }
  // Merge : les overrides du fichier ont priorité sur les défauts
  return { ...DEFAULT_OVERRIDES, ...normalizedFile };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS MÉTIER (existants v7 + nouveaux v8)
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

// ─── NOUVEAU v8 : Anti-prod québécoise locale ─────────────────────────────────
/**
 * Détecte une production québécoise locale qu'on veut filtrer.
 * Critères : pays = CA, langue originale = fr, PAS de coproduction FR/US/UK,
 * et faible portée (budget OU popularité faibles).
 *
 * Un blockbuster US doublé VFQ ne sera PAS détecté ici car son pays = US.
 * Un Xavier Dolan avec coprod française passera aussi (coprod FR détectée).
 */
function isQuebecLocalProduction(details) {
  const countries = details.production_countries?.map((c) => c.iso_3166_1) || [];
  const isCanadian   = countries.includes('CA');
  if (!isCanadian) return false;

  const isFrenchLang = details.original_language === 'fr';
  if (!isFrenchLang) return false;

  // Coproduction internationale "noble" : on garde
  const hasIntlCoprod = countries.some((c) => ['FR', 'US', 'GB', 'BE', 'DE', 'IT', 'ES'].includes(c));
  if (hasIntlCoprod) return false;

  // Faible portée = prod locale Québec
  const lowBudget   = (details.budget ?? 0) < 5_000_000;
  const lowReach    = (details.popularity ?? 0) < 8;
  const fewVotes    = (details.vote_count ?? 0) < 100;

  return lowBudget && (lowReach || fewVotes);
}

// ─── NOUVEAU v8 : Système de tiers ─────────────────────────────────────────────
/**
 * Classifie un film en blockbuster / mid / niche selon un score composite.
 * Score = somme pondérée de budget, popularité, vote_count, revenue.
 */
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
  if (score >= TIER_THRESHOLDS.BLOCKBUSTER_SCORE) tier = 'blockbuster';
  else if (score >= TIER_THRESHOLDS.MID_SCORE)    tier = 'mid';
  else                                            tier = 'niche';

  return { tier, score, breakdown: { budgetScore, popScore, voteScore, revScore } };
}

// ─── NOUVEAU v8 : Prédiction VOD avec mapping studio ──────────────────────────
function predictVODDate(cinemaDate, details, isFrench) {
  if (isFrench) {
    const vod = new Date(cinemaDate);
    vod.setDate(vod.getDate() + DELAYS.FRENCH);
    return { date: vod, delay: DELAYS.FRENCH, method: 'fr-chronologie' };
  }

  // Cherche le délai studio
  const studios = details.production_companies || [];
  const studioDelays = studios
    .map((s) => ({ id: s.id, name: s.name, delay: STUDIO_VOD_DELAYS[s.id] }))
    .filter((s) => s.delay !== undefined);

  if (studioDelays.length > 0) {
    // Le studio "lead" (fenêtre la plus longue) dicte
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

  // Fallback : délai générique US
  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + DELAYS.AMERICAN);
  return { date: vod, delay: DELAYS.AMERICAN, method: 'us-generic' };
}

// ─── NOUVEAU v8 : Scoring de confiance ─────────────────────────────────────────
/**
 * Calcule un score de confiance pour la date VOD selon les sources disponibles.
 * Retourne { level, score, sources[] }.
 */
function computeConfidence({ source, crossConfirmedBy }) {
  const sources = [source];
  if (crossConfirmedBy && crossConfirmedBy.length) {
    sources.push(...crossConfirmedBy);
  }
  const set = new Set(sources);

  // 1. Override manuel = on a tranché à la main, max confiance
  if (source === 'override-manuel') {
    return { level: 'override', score: 1.0, sources };
  }

  // 2. Officielle TMDB FR : très haute confiance
  if (source === 'officielle-fr') {
    return {
      level: set.has('allocine') ? 'very-high' : 'high',
      score: set.has('allocine') ? 0.98 : 0.95,
      sources,
    };
  }

  // 3. Officielle TMDB US + confirmation FR (AlloCiné) : très bonne triangulation
  if (source === 'officielle-us') {
    if (set.has('allocine')) return { level: 'very-high', score: 0.93, sources };
    if (set.has('maxblizz') && set.has('bingebase')) return { level: 'high', score: 0.91, sources };
    if (set.has('maxblizz'))  return { level: 'high',      score: 0.88, sources };
    if (set.has('bingebase')) return { level: 'high',      score: 0.85, sources };
    return                        { level: 'high',      score: 0.82, sources };
  }

  // 4. MaxBlizz (US) + AlloCiné (FR) qui s'accordent : excellent
  if (source === 'maxblizz') {
    if (set.has('allocine') && set.has('bingebase')) return { level: 'very-high', score: 0.95, sources };
    if (set.has('allocine'))     return { level: 'very-high', score: 0.92, sources };
    if (set.has('bingebase'))    return { level: 'high',      score: 0.82, sources };
    if (set.has('studio-mapped'))return { level: 'high',      score: 0.78, sources };
    return                            { level: 'medium',    score: 0.70, sources };
  }

  // 4b. BingeBase seul comme source principale
  if (source === 'bingebase') {
    if (set.has('allocine') && set.has('maxblizz')) return { level: 'very-high', score: 0.93, sources };
    if (set.has('allocine'))     return { level: 'high',      score: 0.85, sources };
    if (set.has('maxblizz'))     return { level: 'high',      score: 0.82, sources };
    return                            { level: 'medium',    score: 0.65, sources };
  }

  // 5. AlloCiné seul (sortie FR officielle annoncée)
  if (source === 'allocine') {
    return { level: 'high', score: 0.80, sources };
  }

  // 6. Prédite avec mapping studio (assez fiable pour blockbusters connus)
  if (source === 'studio-mapped') {
    if (set.has('allocine'))     return { level: 'medium', score: 0.75, sources };
    if (set.has('maxblizz'))     return { level: 'medium', score: 0.72, sources };
    return                            { level: 'medium', score: 0.62, sources };
  }

  // 7. Prédite générique (délai 45j/120j sans info studio)
  return { level: 'low', score: 0.45, sources };
}

// ─── Normalisation titre ──────────────────────────────────────────────────────
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
  // 🆕 v8.2 : fenêtre FR ajustée pour capter les sorties tardives du mois
  // qui auraient leur VOD à 120j tomber dans le mois cible.
  // Ex : un film sorti le 28 janvier 2026 → VOD le 28 mai 2026.
  // frEnd doit donc être ≥ monthStart - (120 - durée_mois). Marge: monthStart - 85j.
  const frStart = new Date(windowEnd);
  frStart.setMonth(frStart.getMonth() - 6); frStart.setDate(frStart.getDate() - 15);
  const frEnd   = new Date(monthStart); frEnd.setDate(frEnd.getDate() - 85);

  const intlStart = new Date(windowEnd);
  intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd   = new Date(monthStart); intlEnd.setDate(intlEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false`;
  return [
    { name: 'FR/origin/popular',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/quality',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=vote_average.desc&vote_count.gte=10` },
    { name: 'FR/lang+region',
      url: `${base}&with_original_language=fr&region=FR&with_release_type=2|3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/recent',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },
    { name: 'FR/theatrical-net',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/popular',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/quality',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=vote_average.desc&vote_count.gte=40` },
    { name: 'INTL/fresh',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=primary_release_date.desc` },
  ];
}

// ═══════════════════════════════════════════════════════════════════════════════
// DÉTAILS FILM (TMDB avec cache)
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isCacheEntryFresh(cache[key])) {
    return cache[key].data;
  }
  // append_to_response : release_dates pour la chronologie + on a déjà
  // production_companies/budget/revenue via /movie/{id} natif
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
// MODULE MAXBLIZZ (scrape + extraction date VOD US — hérité v7)
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

  if (!cacheValid && cache._parserVersion !== undefined) {
    log(`  🔄 MaxBlizz : invalidation cache (parser v${cache._parserVersion} → v${PARSER_VERSION})`);
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
    log(`     ${articles.size} articles VOD trouvés sur la liste`);

    let fromCache = 0, fetched = 0, skipped = 0;
    const newArticleCache = {};
    const monthPattern = '(January|February|March|April|May|June|July|August|September|October|November|December)';
    const datePart = `${monthPattern}\\s+(\\d{1,2}),?\\s+(\\d{4})`;

    for (const [url, slug] of articles) {
      let articleData;
      const cached = articleCache[url];
      const cachedFresh = cached && (Date.now() - cached._cachedAt) / 3600000 < 24 * 7;

      if (cachedFresh) {
        articleData = cached;
        fromCache++;
      } else {
        try {
          await sleep(250);
          const html = await fetchTextWithRetry(url, 2);
          const strongDateRegex = new RegExp(
            `(?:<strong>|\\*\\*)\\s*${datePart}\\s*(?:</strong>|\\*\\*)`, 'gi'
          );
          const contextDateRegex = new RegExp(
            `(?:starting|available\\s+(?:on|to)|rent\\s+(?:on|it)|buy\\s+(?:on|it)|arrives?\\s+on|arriving\\s+on|release\\s+date\\s+(?:is|will\\s+be))\\s+${datePart}`,
            'gi'
          );
          const body = html.length > 3000 ? html.slice(2000) : html;
          const candidates = [];
          let dm;
          while ((dm = strongDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'strong', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          while ((dm = contextDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'context', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          const valid = candidates.filter((c) =>
            !isNaN(c.date) &&
            c.date.getTime() >= Date.now() - 60 * 86400000 &&
            c.date.getTime() <= Date.now() + 730 * 86400000
          );
          let chosenDate = null;
          const strongs  = valid.filter((c) => c.src === 'strong');
          const contexts = valid.filter((c) => c.src === 'context');
          if (strongs.length > 0)       chosenDate = strongs.sort((a, b) => a.date - b.date)[0].date;
          else if (contexts.length > 0) chosenDate = contexts.sort((a, b) => a.date - b.date)[0].date;
          articleData = { _cachedAt: Date.now(), date: chosenDate ? chosenDate.toISOString() : null, slug };
          fetched++;
        } catch (err) {
          articleData = { _cachedAt: Date.now(), date: null, slug, error: err.message };
        }
      }

      newArticleCache[url] = articleData;
      if (!articleData.date) { skipped++; continue; }
      const date = new Date(articleData.date);
      if (isNaN(date)) { skipped++; continue; }
      const title = slug.replace(/-/g, ' ');
      releases.push({ slug, title, date, url });
    }

    log(`     ✓ ${releases.length} films extraits (${fromCache} cache, ${fetched} fetchés, ${skipped} sans date)`);
    saveMaxblizzCache({
      _parserVersion: PARSER_VERSION,
      _listCachedAt : Date.now(),
      articles      : newArticleCache,
      releases      : releases.map((r) => ({ ...r, date: r.date.toISOString() })),
    });
  } catch (err) {
    console.warn(`  ⚠️  MaxBlizz scraping a échoué : ${err.message} — on continue sans.`);
  }
  return releases;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 🆕 MODULE ALLOCINE (NOUVEAU v8) — Triangulation FR
// ═══════════════════════════════════════════════════════════════════════════════
//
// Stratégie :
//  1. Scrape les pages AlloCiné des sorties VOD ("à louer" + "à acheter")
//  2. Extraction des titres + dates de sortie VOD FR
//  3. Cross-référence avec TMDB pour récupérer ID, poster, genres
//  4. Deux usages :
//     a) Ajouter à la liste finale si pas déjà présent (sortie FR officielle)
//     b) Cross-confirmer une date MaxBlizz/TMDB existante (boost de confiance)

const ALLOCINE_URLS = [
  'https://www.allocine.fr/video/aladelocation/',
  'https://www.allocine.fr/video/aladevente/',
];

const FR_MONTHS = {
  'janvier'  :  0, 'février' :  1, 'fevrier' :  1, 'mars'     :  2,
  'avril'    :  3, 'mai'     :  4, 'juin'    :  5, 'juillet'  :  6,
  'août'     :  7, 'aout'    :  7, 'septembre':  8, 'octobre' :  9,
  'novembre' : 10, 'décembre': 11, 'decembre': 11,
};

/**
 * Parse une date FR sous forme texte : "15 mai 2026", "1er avril 2026",
 * "le 5 juin", "à partir du 12 mai 2026", etc.
 * Retourne un Date ou null. Si l'année n'est pas explicite, on prend l'année courante.
 */
function parseFrenchDate(text, fallbackYear = new Date().getFullYear()) {
  if (!text) return null;
  const cleaned = text.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // enlève accents pour matcher
    .replace(/1er/g, '1');
  // Cherche "JJ mois AAAA?" — pas trop greedy
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
  if (!candidates.length) return null;
  return candidates.sort((a, b) => a - b)[0];
}

/**
 * Extrait des { title, date } depuis le HTML d'une page AlloCiné VOD.
 * Defensif : essaie plusieurs patterns pour résister aux changements de structure.
 */
function parseAllocineHtml(html) {
  const items = [];
  const seenTitles = new Set();

  // Pattern 1 : structure "fiche film" type meta-title + meta-date
  // <a class="meta-title-link" href="/film/fichefilm_gen_cfilm=NNNN.html">Titre</a>
  // ... "Sortie le 5 mai 2026" ou "VOD le 5 mai 2026"
  const filmCardRegex = /<a[^>]*class="[^"]*meta-title-link[^"]*"[^>]*>([^<]{2,100})<\/a>([\s\S]{0,800}?)(?=<a[^>]*class="[^"]*meta-title-link|$)/gi;
  let m;
  while ((m = filmCardRegex.exec(html)) !== null) {
    const rawTitle = m[1].trim();
    const context  = m[2];
    if (!rawTitle || seenTitles.has(rawTitle.toLowerCase())) continue;

    // Recherche d'une date FR dans le contexte
    const date = parseFrenchDate(context);
    if (!date) continue;
    seenTitles.add(rawTitle.toLowerCase());
    items.push({ title: rawTitle, date });
  }

  // Pattern 2 : fallback générique — toute balise <a> avec titre + "Sortie ... <date>"
  // (utile si AlloCiné refactor la classe CSS)
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

/**
 * Récupère les sorties VOD FR annoncées sur AlloCiné.
 * Cache léger (TTL court car les annonces FR peuvent changer).
 */
async function fetchAllocineReleases() {
  const PARSER_VERSION = 1;
  const cache = loadAllocineCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const fresh = cacheValid && cache._cachedAt
    && (Date.now() - cache._cachedAt) / 3600000 < ALLOCINE_TTL_HOURS;

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
      vlog(`     ${path.basename(url, '/')} : ${items.length} films extraits`);
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

  log(`     ✓ ${releases.length} films AlloCiné extraits (dédupliqués)`);
  saveAllocineCache({
    _parserVersion: PARSER_VERSION,
    _cachedAt: Date.now(),
    releases: releases.map((r) => ({ ...r, date: r.date.toISOString() })),
  });

  return releases;
}

/**
 * Cross-confirme et enrichit la liste finale avec les données AlloCiné.
 * - Si un film de finalResults match un film AlloCiné → ajoute "allocine" aux sources
 *   et éventuellement corrige la date (si la date AlloCiné diffère, AlloCiné = source FR
 *   officielle donc priorité sur la prédiction)
 * - Si un film AlloCiné n'est pas dans la liste et tombe dans le mois cible → ajout
 */
async function enrichWithAllocine({ finalResults, monthStart, monthEnd, cache, tier_filter }) {
  log('\n  📡  Cross-référence AlloCiné (triangulation FR) :');
  const allocineReleases = await fetchAllocineReleases();
  if (allocineReleases.length === 0) {
    log('     (aucune donnée AlloCiné — on saute)');
    return { confirmed: 0, added: 0, corrected: 0 };
  }

  const existingByTitle = new Map();
  for (const r of finalResults) {
    existingByTitle.set(normalizeTitle(r.title), r);
    if (r.original_title) existingByTitle.set(normalizeTitle(r.original_title), r);
  }

  let confirmed = 0, added = 0, corrected = 0, skipped = 0;

  for (const ac of allocineReleases) {
    // Le film AlloCiné doit tomber dans le mois cible (sinon on ignore)
    if (ac.date < monthStart || ac.date > monthEnd) { skipped++; continue; }

    const key = normalizeTitle(ac.title);
    const existing = existingByTitle.get(key);

    if (existing) {
      // Cross-confirmation : ajoute "allocine" aux sources
      existing._crossConfirmedBy = existing._crossConfirmedBy || [];
      if (!existing._crossConfirmedBy.includes('allocine')) {
        existing._crossConfirmedBy.push('allocine');
        confirmed++;

        // Si la date AlloCiné diffère de plus de 5 jours, on corrige
        // (AlloCiné = source FR officielle, prime sur les prédictions)
        const diffDays = Math.abs(existing._sortDate - ac.date.getTime()) / 86400000;
        if (diffDays > 5 && (existing.source === 'studio-mapped' || existing.source === 'prédite' || existing.source === 'maxblizz')) {
          const oldDate = existing.plex_release;
          existing.plex_release = formatDateFR(ac.date);
          existing._sortDate    = ac.date.getTime();
          existing.source       = 'officielle-allocine';
          corrected++;
          log(`     ↻ "${existing.title}" : ${oldDate} → ${formatDateFR(ac.date)} (AlloCiné prime)`);
        } else {
          vlog(`     ✓ Confirmation "${existing.title}" → ${formatDateFR(ac.date)}`);
        }
      }
      continue;
    }

    // Pas dans la liste : tentative d'ajout via recherche TMDB
    const tmdbHit = await tmdbSearchByTitle(ac.title);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) {
      vlog(`     ✗ TMDB sans match pour "${ac.title}"`);
      skipped++;
      continue;
    }

    let details;
    try {
      details = await fetchMovieDetails(tmdbHit.id, cache);
      await sleep(API_DELAY_MS);
    } catch {
      skipped++; continue;
    }

    // Mêmes filtres anti-bruit que le pipeline principal
    if (hasExcludedGenre(details))      { skipped++; continue; }
    if (isTelefilmByTitle(details))     { skipped++; continue; }
    if (isSpectacle(details))           { skipped++; continue; }
    if (isQuebecLocalProduction(details)){ skipped++; continue; }

    // Tier filter : on ne veut pas de niche non-français en ajout AlloCiné non plus
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
// 🆕 MODULE BINGEBASE (v8.4) — Source US : sorties digitales du mois en cours
// ═══════════════════════════════════════════════════════════════════════════════
//
// Stratégie :
//  1. Construction dynamique de l'URL mensuelle :
//     https://bingebase.com/releases/digital/[month]-[year]
//     Ex : https://bingebase.com/releases/digital/june-2026
//  2. Parse la page Markdown-like :
//     ## Monday, June 9, 2025    → section de date
//     ### PG-13 Title (year)     → titre de film
//     Extracte : titre nettoyé (sans rating) + date de sortie
//  3. Rôle :
//     a) Cross-confirmer une entrée MaxBlizz ou TMDB existante
//        (ajoute "bingebase" aux crossConfirmedBy → boost confiance)
//     b) Ajouter les films US manquants non détectés par MaxBlizz
//        (mêmes filtres tier/QC/genre que les autres sources)
//  4. Cache dédié BINGEBASE_CACHE avec TTL 12h.

const BINGEBASE_MONTH_EN = [
  'january','february','march','april','may','june',
  'july','august','september','october','november','december',
];

// Ratings MPAA à retirer du début des titres BingeBase (### PG-13 Title)
const BINGEBASE_RATING_PREFIX = /^(G|PG|PG-13|R|NC-17|NR|UR|TV-G|TV-PG|TV-14|TV-MA)\s+/i;

/**
 * Construit l'URL BingeBase pour le mois en cours (ou un mois donné).
 * @param {Date} [now]
 * @returns {string}
 */
function buildBingebaseUrl(now = new Date()) {
  const month = BINGEBASE_MONTH_EN[now.getMonth()];
  const year  = now.getFullYear();
  return `https://bingebase.com/releases/digital/${month}-${year}`;
}

/**
 * Parse le HTML/Markdown de BingeBase pour extraire { title, date }[].
 *
 * Structure observée :
 *   ## [Weekday], [Month] [D], [Year]
 *   ...
 *   ### [Rating?] [Title] ([year?])
 *
 * On peut aussi avoir des sections <h2> et <h3> en HTML brut, selon le renderer.
 * Le parser gère les deux formes (Markdown-like et balises HTML).
 */
function parseBingebaseHtml(html) {
  const items   = [];
  const seen    = new Set();
  let   current = null; // Date courante de la section ## en cours

  // ── Nettoyage léger du HTML pour simplifier le parsing ───────────────────────
  // Supprime les balises inline (strong, em, a, span) mais garde le texte
  const clean = html
    .replace(/<a[^>]*>([\s\S]*?)<\/a>/gi, '$1')
    .replace(/<\/?(strong|em|span|b|i|code)[^>]*>/gi, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#\d+;/g, ' ')
    .replace(/&[a-z]+;/g, ' ');

  // ── Pattern pour les sections de date (h2) ───────────────────────────────────
  // Forme Markdown : ## Monday, June 9, 2026
  // Forme HTML    : <h2>Monday, June 9, 2026</h2>
  const EN_MONTHS_MAP = {
    january:0, february:1, march:2, april:3, may:4, june:5,
    july:6, august:7, september:8, october:9, november:10, december:11,
  };

  // Regex globale qui capture alternativement la forme Markdown et la forme HTML
  const sectionRx = /(?:^|\n)(?:##\s+|<h2[^>]*>)((?:monday|tuesday|wednesday|thursday|friday|saturday|sunday),\s+([a-z]+)\s+(\d{1,2}),?\s+(\d{4}))(?:<\/h2>)?/gim;

  // Regex pour les titres de films (h3)
  // Forme Markdown : ### PG-13 Avengers: Doomsday (2026)
  // Forme HTML     : <h3>PG-13 Avengers: Doomsday (2026)</h3>
  const filmRx = /(?:^|\n)(?:###\s+|<h3[^>]*>)(.+?)(?:<\/h3>)?(?:\n|$)/gim;

  // On parcourt le texte en cherchant alternativement sections et films
  // Stratégie : on collecte les positions de chaque token, on trie par position
  // puis on reconstitue les groupes date → films.

  const tokens = [];

  let m;
  while ((m = sectionRx.exec(clean)) !== null) {
    const monthStr = m[2]?.toLowerCase();
    const day      = parseInt(m[3], 10);
    const year     = parseInt(m[4], 10);
    const monthIdx = EN_MONTHS_MAP[monthStr];
    if (monthIdx === undefined || isNaN(day) || isNaN(year)) continue;
    const date = new Date(Date.UTC(year, monthIdx, day, 12, 0, 0));
    if (isNaN(date)) continue;
    tokens.push({ type: 'date', pos: m.index, date });
  }

  while ((m = filmRx.exec(clean)) !== null) {
    const raw = m[1]?.trim();
    if (!raw || raw.length < 2) continue;
    // Retire le rating MPAA du début
    const titleRaw = raw.replace(BINGEBASE_RATING_PREFIX, '').trim();
    // Retire l'année entre parenthèses en fin : "Title (2026)" → "Title"
    const title = titleRaw.replace(/\s*\(\d{4}\)\s*$/, '').trim();
    if (!title || title.length < 2) continue;
    tokens.push({ type: 'film', pos: m.index, title });
  }

  // Tri par position dans le document
  tokens.sort((a, b) => a.pos - b.pos);

  // Reconstitution : chaque film hérite de la date de la dernière section rencontrée
  for (const tok of tokens) {
    if (tok.type === 'date') {
      current = tok.date;
    } else if (tok.type === 'film' && current) {
      const key = tok.title.toLowerCase().trim();
      if (seen.has(key)) continue;
      seen.add(key);
      items.push({ title: tok.title, date: current });
    }
  }

  return items;
}

/**
 * Scrape BingeBase pour le mois en cours et retourne { title, date }[].
 * Utilise un cache local TTL 12h.
 * @param {Date} [now]
 * @returns {Promise<Array<{title:string, date:Date}>>}
 */
async function fetchBingebaseReleases(now = new Date()) {
  const PARSER_VERSION = 1;
  const cache      = loadBingebaseCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const fresh      = cacheValid && cache._cachedAt
    && (Date.now() - cache._cachedAt) / 3600000 < BINGEBASE_TTL_HOURS;

  if (fresh && Array.isArray(cache.releases)) {
    log(`  💾 BingeBase : cache valide (${cache.releases.length} entrées)`);
    return cache.releases.map((r) => ({ ...r, date: new Date(r.date) }));
  }

  const url = buildBingebaseUrl(now);
  log(`  🌐 BingeBase : scraping en cours… ${url}`);
  const releases = [];

  try {
    await sleep(300);
    const html  = await fetchTextWithRetry(url, 2);
    const items = parseBingebaseHtml(html);
    log(`     ${items.length} films extraits de BingeBase`);

    for (const it of items) {
      releases.push({ title: it.title, date: it.date });
    }

    saveBingebaseCache({
      _parserVersion : PARSER_VERSION,
      _cachedAt      : Date.now(),
      _url           : url,
      releases       : releases.map((r) => ({ ...r, date: r.date.toISOString() })),
    });

    log(`  ✓  BingeBase : ${releases.length} sorties digitales enregistrées`);
  } catch (err) {
    console.warn(`  ⚠️  BingeBase scraping a échoué : ${err.message} — on continue sans.`);
  }

  return releases;
}

/**
 * Phase 3.5 — Enrichissement avec BingeBase.
 * Même logique que enrichWithMaxblizz :
 *  - Cross-confirmation si le film est déjà dans finalResults
 *  - Ajout si absent (avec tous les filtres qualité)
 *  - PAS de correction de date (BingeBase = date US, on utilise seulement
 *    pour cross-confirmation ; la date FR reste prioritaire).
 */
async function enrichWithBingebase({ finalResults, monthStart, monthEnd, cache, overrides }) {
  log('\n  📡  Enrichissement BingeBase (US digital) :');
  const bbReleases = await fetchBingebaseReleases();

  if (bbReleases.length === 0) {
    log('     (aucune donnée BingeBase — on saute)');
    return { confirmed: 0, added: 0, skipped: 0 };
  }

  // Index des résultats existants
  const existingById     = new Map(finalResults.map((m) => [m.tmdb_id, m]));
  const existingByTitle  = new Map();
  for (const r of finalResults) {
    existingByTitle.set(normalizeTitle(r.title), r);
    if (r.original_title) existingByTitle.set(normalizeTitle(r.original_title), r);
  }

  let confirmed = 0, added = 0, skipped = 0, dropQc = 0, dropNiche = 0;

  for (const bb of bbReleases) {
    // Filtre fenêtre : on accepte ±15 jours autour du mois cible côté US
    // (les dates US sont légèrement en avance sur les dates FR VOD)
    const windowMin = new Date(monthStart); windowMin.setDate(windowMin.getDate() - 15);
    const windowMax = new Date(monthEnd);   windowMax.setDate(windowMax.getDate() + 15);
    if (bb.date < windowMin || bb.date > windowMax) { skipped++; continue; }

    const key      = normalizeTitle(bb.title);
    const existing = existingByTitle.get(key);

    if (existing) {
      // ── Cross-confirmation ─────────────────────────────────────────────────
      existing._crossConfirmedBy = existing._crossConfirmedBy || [];
      if (!existing._crossConfirmedBy.includes('bingebase')) {
        existing._crossConfirmedBy.push('bingebase');
        confirmed++;
        vlog(`     ✓ Confirmation BingeBase "${existing.title}" (date US ${bb.date.toISOString().slice(0,10)})`);
      }
      continue;
    }

    // ── Tentative d'ajout via TMDB ────────────────────────────────────────────
    const tmdbHit = await tmdbSearchByTitle(bb.title);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) {
      vlog(`     ✗ TMDB sans match pour BingeBase "${bb.title}"`);
      skipped++;
      continue;
    }

    // Déjà présent par ID TMDB (titre légèrement différent)
    if (existingById.has(tmdbHit.id)) {
      const ex = existingById.get(tmdbHit.id);
      ex._crossConfirmedBy = ex._crossConfirmedBy || [];
      if (!ex._crossConfirmedBy.includes('bingebase')) {
        ex._crossConfirmedBy.push('bingebase');
        confirmed++;
        vlog(`     ✓ Confirmation BingeBase (ID match) "${ex.title}"`);
      }
      continue;
    }

    let details;
    try {
      details = await fetchMovieDetails(tmdbHit.id, cache);
      await sleep(API_DELAY_MS);
    } catch {
      skipped++; continue;
    }

    // ── Filtres qualité (mêmes règles que le pipeline principal) ─────────────
    if (hasExcludedGenre(details))       { skipped++; continue; }
    if (isTelefilmByTitle(details))      { skipped++; continue; }
    if (isSpectacle(details))            { skipped++; continue; }
    if (isGhostEntry(details))           { skipped++; continue; }
    if (isQuebecLocalProduction(details)){ dropQc++; continue; }

    const isFrench = isFrenchProduction(details);
    const tierInfo = classifyTier(details);
    if (!isFrench && tierInfo.tier === 'niche') { dropNiche++; continue; }

    // ── Date VOD FR estimée depuis la date US BingeBase ──────────────────────
    // BingeBase donne la date US. Pour la FR on applique +15j en moyenne
    // (fenêtre US → FR typiquement 7-21j selon studio).
    // Si MaxBlizz ou AlloCiné ont déjà une date, elles prendront le dessus
    // en phase 3 / phase 4 grâce au mécanisme crossConfirmedBy.
    const rawVodDate = new Date(bb.date);
    rawVodDate.setDate(rawVodDate.getDate() + 15); // décalage US→FR estimé

    // On vérifie que la date FR estimée tombe dans le mois cible
    if (rawVodDate < monthStart || rawVodDate > monthEnd) { skipped++; continue; }

    // Applique un override manuel si présent
    let vodDate = rawVodDate;
    let sourceStr = 'bingebase';
    const titleNorm = normalizeTitle(details.title || tmdbHit.title);
    const origNorm  = normalizeTitle(details.original_title || '');
    const ov = overrides?.[titleNorm] || overrides?.[origNorm];
    if (ov) {
      const ovDate = new Date(ov.date);
      if (!isNaN(ovDate)) {
        vodDate   = ovDate;
        sourceStr = 'override-manuel';
        log(`     ★ Override BingeBase "${details.title || tmdbHit.title}" → ${formatDateFR(vodDate)} (${ov.reason})`);
      }
    }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? (details.release_date ? new Date(details.release_date) : null);
    const studios      = details.production_companies || [];
    const leadStudio   = studios.find((s) => STUDIO_VOD_DELAYS[s.id]);

    const entry = {
      title          : details.title || tmdbHit.title,
      plex_release   : formatDateFR(vodDate),
      tmdb_id        : tmdbHit.id,
      poster_path    : details.poster_path || tmdbHit.poster_path,
      original_title : details.original_title || tmdbHit.original_title,
      cinema_date    : cinemaDate ? formatDateFR(cinemaDate) : null,
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : details.vote_count ?? 0,
      genres         : (details.genres || []).map((g) => g.name),
      is_french      : isFrench,
      source         : sourceStr,
      _tier          : tierInfo.tier,
      _tierScore     : tierInfo.score,
      _leadStudio    : leadStudio?.name || null,
      _sortDate      : vodDate.getTime(),
      _popularity    : details.popularity ?? tmdbHit.popularity ?? 0,
      _crossConfirmedBy: ['bingebase'],
    };

    finalResults.push(entry);
    existingById.set(tmdbHit.id, entry);
    existingByTitle.set(normalizeTitle(entry.title), entry);

    log(`     ➕ Ajouté via BingeBase : ${entry.title} → ${formatDateFR(vodDate)} [${tierInfo.tier}]`);
    added++;
  }

  log(`     📊 ${confirmed} confirmations, ${added} ajouts, ${dropQc} QC locaux jetés, ${dropNiche} niche non-FR jetés, ${skipped} ignorés`);
  return { confirmed, added, skipped };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MODULE MAXBLIZZ — Enrichissement (hérité v7, étendu pour tier + overrides ext.)
// ═══════════════════════════════════════════════════════════════════════════════

async function enrichWithMaxblizz({ finalResults, monthStart, monthEnd, cache, overrides }) {
  log('\n  📡  Enrichissement MaxBlizz :');
  const mbReleases = await fetchMaxblizzReleases();
  if (mbReleases.length === 0) return { added: 0, overridden: 0 };

  const existingIds       = new Set(finalResults.map((m) => m.tmdb_id));
  const existingTitles    = new Set(finalResults.map((m) => normalizeTitle(m.title)));
  const existingOriginals = new Set(finalResults.map((m) => normalizeTitle(m.original_title || '')));

  let added = 0, overridden = 0, skipped = 0, dropQc = 0, dropNiche = 0;

  for (const mb of mbReleases) {
    const slugNorm = normalizeTitle(mb.title);
    let mbDate = mb.date;
    let isOverride = false;

    // Applique l'override manuel si présent
    const tokens = slugNorm.split(' ');
    const overrideKeys = [slugNorm];
    if (tokens.length >= 3) overrideKeys.push(tokens.slice(1).join(' '));
    if (tokens.length >= 4) overrideKeys.push(tokens.slice(2).join(' '));
    for (const k of overrideKeys) {
      if (overrides[k]) {
        mbDate = new Date(overrides[k].date);
        isOverride = true;
        log(`     ★ Override "${mb.title}" → ${formatDateFR(mbDate)} (${overrides[k].reason})`);
        break;
      }
    }

    if (mbDate < monthStart || mbDate > monthEnd) { skipped++; continue; }

    const tmdbHit = await tmdbSearchByTitle(mb.title);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) {
      vlog(`     ✗ TMDB sans match pour "${mb.title}"`);
      skipped++;
      continue;
    }

    // Déjà présent par TMDB ID : on peut corriger la date si MaxBlizz est plus précis
    if (existingIds.has(tmdbHit.id)) {
      const existing = finalResults.find((m) => m.tmdb_id === tmdbHit.id);
      existing._crossConfirmedBy = existing._crossConfirmedBy || [];
      if (!existing._crossConfirmedBy.includes('maxblizz')) {
        existing._crossConfirmedBy.push('maxblizz');
      }
      const oldDate = existing.plex_release;
      if (existing.plex_release !== formatDateFR(mbDate)) {
        existing.plex_release = formatDateFR(mbDate);
        existing._sortDate    = mbDate.getTime();
        existing.source       = isOverride ? 'override-manuel' : 'maxblizz';
        log(`     ↻ "${tmdbHit.title}" : ${oldDate} → ${formatDateFR(mbDate)} (maxblizz)`);
        overridden++;
      }
      continue;
    }

    // Garde-fou titres
    const tmdbTitleNorm = normalizeTitle(tmdbHit.title);
    const tmdbOrigNorm  = normalizeTitle(tmdbHit.original_title || '');
    if (existingTitles.has(tmdbTitleNorm) || existingOriginals.has(tmdbOrigNorm)) {
      skipped++; continue;
    }

    let details;
    try {
      details = await fetchMovieDetails(tmdbHit.id, cache);
      await sleep(API_DELAY_MS);
    } catch {
      skipped++; continue;
    }

    // 🆕 v8 : filtre prod québécoise locale
    if (isQuebecLocalProduction(details)) {
      vlog(`     🚫 "${tmdbHit.title}" filtré (prod QC locale)`);
      dropQc++; continue;
    }

    // 🆕 v8 : filtre tier pour non-FR (on ne veut PAS de niche international)
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
      plex_release   : formatDateFR(mbDate),
      tmdb_id        : tmdbHit.id,
      poster_path    : details.poster_path || tmdbHit.poster_path,
      original_title : details.original_title || tmdbHit.original_title,
      cinema_date    : cinemaDate ? formatDateFR(cinemaDate) : null,
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : details.vote_count ?? 0,
      genres         : (details.genres || []).map((g) => g.name),
      is_french      : isFrench,
      source         : isOverride ? 'override-manuel' : 'maxblizz',
      _tier          : tierInfo.tier,
      _tierScore     : tierInfo.score,
      _leadStudio    : leadStudio?.name || null,
      _sortDate      : mbDate.getTime(),
      _popularity    : details.popularity ?? tmdbHit.popularity ?? 0,
      _crossConfirmedBy: [],
    });

    log(`     ✓ Ajouté : ${details.title || tmdbHit.title} → ${formatDateFR(mbDate)} [${tierInfo.tier}]`);
    added++;
    existingIds.add(tmdbHit.id);
  }

  log(`     📊 ${added} ajouts, ${overridden} dates corrigées, ${dropQc} QC locaux jetés, ${dropNiche} niche non-FR jetés, ${skipped} ignorés`);
  return { added, overridden };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE PRINCIPAL
// ═══════════════════════════════════════════════════════════════════════════════

async function updateVOD() {
  const t0 = Date.now();
  log('🎬  updateVOD v8 (ultimate) — démarrage...');
  if (DRY_RUN)  log('   ⚙️  Mode --dry-run actif : aucune écriture du JSON final');
  if (VERBOSE)  log('   ⚙️  Mode --verbose actif : logs détaillés');
  log('');

  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);
  log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  const cache     = loadCache();
  const overrides = loadOverrides();
  log(`💾  Cache TMDB : ${Object.keys(cache).length} entrées chargées`);
  log(`📋  Overrides chargés : ${Object.keys(overrides).length} entrées (${OVERRIDES_PATH})\n`);

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
    } catch (err) {
      log(`⚠️  ${err.message}`);
    }
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
    quebecLocal: 0,        // 🆕 v8
    nicheNonFrench: 0,     // 🆕 v8
  };
  let cacheHits = 0;

  log('  🔬  Analyse détaillée :');
  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

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

    // 🆕 v8 : Anti-prod québécoise locale
    if (isQuebecLocalProduction(details)) { dropReasons.quebecLocal++; continue; }

    const isFrench  = isFrenchProduction(details);
    const minDelay  = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine = hasTheatricalReleaseFR(details.release_dates);
    if (!hasFRCine) { dropReasons.noFRTheatrical++; continue; }

    // ── Filtre Qualité Composite v6 (préservé) ──────────────────────────────
    const voteCount  = details.vote_count ?? 0;
    const popularity = movie.popularity ?? details.popularity ?? 0;
    const isSafeVolume   = voteCount >= 5;
    const isNicheButReal = popularity >= 1.5;
    if (!isSafeVolume && !isNicheButReal) { dropReasons.lowQuality++; continue; }

    // 🆕 v8 : Tier classification + filtre niche non-français
    const tierInfo = classifyTier(details);
    if (!isFrench && tierInfo.tier === 'niche') {
      vlog(`     🚫 ${details.title} filtré (niche non-FR, score=${tierInfo.score})`);
      dropReasons.nicheNonFrench++;
      continue;
    }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    // 🆕 v8 : Prédiction avec délai par studio
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

    // ── Override manuel : priorité absolue, écrase même une date officielle ──
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

    const actualDelayDays = (vodDate - cinemaDate) / 86400000;
    if (actualDelayDays < minDelay && !officialDigital) {
      // Garde-fou : si la prédiction génère un délai trop court pour la France, on jette
      dropReasons.delayTooShort++; continue;
    }
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

  // ─── Phase 3.5 : 🆕 Enrichissement BingeBase (US digital) ───────────────────
  await enrichWithBingebase({ finalResults, monthStart, monthEnd, cache, overrides });

  // ─── Phase 4 : 🆕 Triangulation AlloCiné ─────────────────────────────────────
  await enrichWithAllocine({ finalResults, monthStart, monthEnd, cache });

  // ─── Phase 5 : Scoring de confiance final ────────────────────────────────────
  log('\n  🎯  Calcul des scores de confiance :');
  for (const item of finalResults) {
    item.confidence = computeConfidence({
      source: item.source,
      crossConfirmedBy: item._crossConfirmedBy || [],
    });
  }

  // ─── Phase 6 : Tri, dédup, sérialisation ─────────────────────────────────────
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });

  const deduped = Array.from(
    new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values()
  );

  // On sépare les champs internes (commençant par _) avant écriture
  const output = deduped.map((m) => {
    const clean = {};
    for (const [k, v] of Object.entries(m)) {
      if (k.startsWith('_')) continue;
      clean[k] = v;
    }
    // On expose le tier et le studio dans l'output (utile pour le front)
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

  // ─── Phase 7 : Cleanup cache & récap ─────────────────────────────────────────
  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  // Récap détaillé
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  const frCount    = output.filter((m) => m.is_french).length;
  const intlCount  = output.length - frCount;
  const byTier     = output.reduce((acc, m) => { acc[m.tier] = (acc[m.tier] || 0) + 1; return acc; }, {});
  const bySource   = output.reduce((acc, m) => { acc[m.source] = (acc[m.source] || 0) + 1; return acc; }, {});
  const byConf     = output.reduce((acc, m) => { acc[m.confidence.level] = (acc[m.confidence.level] || 0) + 1; return acc; }, {});
  const avgConf    = output.length
    ? (output.reduce((s, m) => s + m.confidence.score, 0) / output.length).toFixed(2)
    : 'N/A';

  log('\n  ═══════════════════════════════════════════════════════════════════════');
  log(`  ✅ Terminé en ${elapsed}s — ${output.length} films générés`);
  log(`     🇫🇷 Français : ${frCount}    🌍 International : ${intlCount}`);
  log('  ─────────────────────────────────────────────────────────────────────');
  log(`     Par tier      : ${Object.entries(byTier).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Par source    : ${Object.entries(bySource).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Par confiance : ${Object.entries(byConf).map(([k,v]) => `${k}=${v}`).join('  |  ')}`);
  log(`     Confiance moyenne : ${avgConf} / 1.00`);
  log('  ─────────────────────────────────────────────────────────────────────');
  log(`     Filtres écartés :`);
  log(`        • Qualité faible       : ${dropReasons.lowQuality}`);
  log(`        • Niche non-FR         : ${dropReasons.nicheNonFrench}  🆕`);
  log(`        • Prod québécoise loc. : ${dropReasons.quebecLocal}  🆕`);
  log(`        • Pas de sortie FR     : ${dropReasons.noFRTheatrical}`);
  log(`        • Théâtre/spectacle    : ${dropReasons.spectacle}`);
  log(`        • Téléfilm par titre   : ${dropReasons.telefilmByTitle}`);
  log(`        • Genre exclu          : ${dropReasons.excludedGenre}`);
  log(`        • Fantôme              : ${dropReasons.ghostEntry}`);
  log(`        • Trop court           : ${dropReasons.tooShort}`);
  log(`        • Délai trop court     : ${dropReasons.delayTooShort}`);
  log(`        • Hors fenêtre (avant) : ${dropReasons.beforeWindow}`);
  log(`        • Hors fenêtre (après) : ${dropReasons.afterWindow}`);
  log(`     Cache TMDB hits        : ${cacheHits} / ${uniqueMovies.length}`);
  log('  ═══════════════════════════════════════════════════════════════════════');

  if (DRY_RUN) log('\n  ⚙️  Mode dry-run : le fichier final N\'a PAS été écrit.');
  else         log(`\n  💾  Fichier écrit : ${DATA_PATH}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
