/**
 * updateVOD.js — v7.1
 * ===================
 * Génère la liste des FILMS DE CINÉMA en VOD pour Plex FR.
 * Fenêtre glissante de ±30 jours autour d'aujourd'hui.
 *
 * Patches v7.1 (vs v7.0) :
 * 🔧 Filtre STRICT : vote_count=0 + vote_average=0 → drop (élimine captations fantômes)
 * 🔧 Détecteur spectacle enrichi : pattern "fête ses N ans", distributeurs spectacle vivant
 * 🔧 JustWatch élargi à US/CA pour dates VFQ (utile si TVOD FR pas encore dispo)
 * 🔧 Source "justwatch-na" avec confidence 0.75
 * 🔧 Logs : ⚠️ visible pour les films à confidence < 0.5
 *
 * Améliorations v7 (vs v6) :
 * ✅ Sources multiples : TMDB + Allociné + JustWatch (avec fallbacks gracieux)
 * ✅ Auto-apprentissage des délais par distributeur (médiane glissante)
 * ✅ Score de confiance par film (0–1) pour usage futur côté front
 * ✅ Fix: delayTooShort ne s'applique plus aux dates officielles confirmées
 * ✅ Fix: getOfficialDigitalDate ne fallback plus sur US (polluait les FR)
 * ✅ Fix: isFrenchProduction plus tolérant aux métadonnées TMDB incomplètes
 * ✅ Fix: lowQuality désactivé pour les films FR récents (<150j) à faible volume
 * ✅ Cache versionné : invalidation auto si la logique change
 * ✅ formatDateFR avec fallback (ne casse plus sur Node sans ICU complet)
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * * (tous les soirs à 2h)
 *
 * Outputs:
 *   data/plex-upcoming.json     ← consommé par index.html (contrat préservé)
 *   data/.tmdb-cache.json       ← cache TMDB (TTL 24h)
 *   data/.justwatch-cache.json  ← cache JustWatch (TTL 12h)
 *   data/.allocine-cache.json   ← cache Allociné (TTL 6h, plus volatil)
 *   data/.delay-history.json    ← historique apprentissage délais
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const CACHE_VERSION = 'v7.1'; // bump pour invalider tous les caches
const TMDB_API_KEY  = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';

const DATA_DIR        = path.join(__dirname, '../data');
const DATA_PATH       = path.join(DATA_DIR, 'plex-upcoming.json');
const TMDB_CACHE      = path.join(DATA_DIR, '.tmdb-cache.json');
const JW_CACHE        = path.join(DATA_DIR, '.justwatch-cache.json');
const ALLO_CACHE      = path.join(DATA_DIR, '.allocine-cache.json');
const DELAY_HISTORY   = path.join(DATA_DIR, '.delay-history.json');

// Délais par défaut (médianes empiriques 2024-2025) — utilisés si pas assez d'historique
const DEFAULT_DELAYS = {
  FRENCH:   120,  // Chronologie médias FR (TVOD à l'acte)
  AMERICAN:  45,  // Moyenne PVOD/TVOD US, mais varie énormément par distributeur
};

// Délais empiriques connus par distributeur (point de départ avant apprentissage)
// Ces valeurs sont MISES À JOUR automatiquement par le système d'apprentissage
const DISTRIBUTOR_HINTS = {
  'Universal Pictures':         { days: 31, country: 'US' },
  'Focus Features':             { days: 31, country: 'US' },
  'Walt Disney Pictures':       { days: 70, country: 'US' },
  'Pixar':                      { days: 70, country: 'US' },
  'Marvel Studios':             { days: 75, country: 'US' },
  'Warner Bros. Pictures':      { days: 50, country: 'US' },
  'Sony Pictures':              { days: 45, country: 'US' },
  'Columbia Pictures':          { days: 45, country: 'US' },
  'Paramount Pictures':         { days: 45, country: 'US' },
  'A24':                        { days: 60, country: 'US' },
  'Lionsgate':                  { days: 60, country: 'US' },
  'Apple Original Films':       { days: 90, country: 'US' },
};

// Apprentissage : nombre minimum d'échantillons pour faire confiance à un délai appris
const LEARNING_MIN_SAMPLES = 5;
// Cache TTLs (en heures)
const TMDB_TTL_H = 24;
const JW_TTL_H   = 12;
const ALLO_TTL_H = 6;
// Délai entre appels API (anti rate-limit)
const API_DELAY_MS = 130;
const ALLO_DELAY_MS = 600; // Plus conservateur pour le scraping
const MAX_PAGES_PER_ENDPOINT = 6;
const MIN_RUNTIME = 40;
const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]); // Doc, Music, TV Movie

// Fenêtre glissante : N jours avant et après aujourd'hui
const WINDOW_DAYS_BEFORE = 30;
const WINDOW_DAYS_AFTER  = 30;

const SPECTACLE_KEYWORDS = [
  'symphony', 'symphonie', 'philharmonic', 'philharmonique',
  ' opera', 'opera:', "l'opéra", 'opéra de paris', 'paris opera',
  'in concert', 'live at', 'live in', 'live from', 'en concert',
  ' tour ', 'world tour', 'la tournée',
  ' ballet', 'casse-noisette', 'nutcracker', 'der nussknacker',
  'récital', 'recital', 'metropolitan opera', 'royal opera',
  'gaming x symphony',
  'théâtre', 'theatre', 'pièce de', 'comédie française', 'captation',
  'molière', 'charbon dans les veines',
];

// v7.1 : Patterns titres typiques des captations stand-up / spectacle vivant
const SPECTACLE_TITLE_PATTERNS = [
  /fête ses?\s+\d+\s+ans/i,        // "Booder fête ses 20 ans"
  /\bau\s+(zénith|olympia|casino de paris|grand rex|palais des sports)\b/i,
  /\bsur scène\b/i,
  /\bone\s*[- ]?\s*(wo)?man\s*[- ]?\s*show\b/i,
  /\bspectacle\b/i,
  /\bstand[- ]?up\b/i,
];

// v7.1 : Distributeurs spectacle vivant connus → drop direct
const SPECTACLE_DISTRIBUTORS = new Set([
  'Arpagon Productions',
  'Pascal Légitimus Productions',
  'JMD Production',
  'Ki M\'aime Me Suive',
  'Productions du Théâtre',
  'Théâtre des Nouveautés',
  'Comédie Française',
]);

const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

// ─── HELPERS DE BASE ──────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url, opts = {}, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, opts);
      if (res.status === 429) { await sleep(2000 * attempt); continue; }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const ct = res.headers.get('content-type') || '';
      return ct.includes('json') ? await res.json() : await res.text();
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

// Format date robuste (fallback si Intl absent)
function formatDateFR(date) {
  try {
    return date.toLocaleDateString('fr-FR', {
      day: 'numeric', month: 'long', year: 'numeric',
    });
  } catch {
    const months = ['Janvier','Février','Mars','Avril','Mai','Juin',
                    'Juillet','Août','Septembre','Octobre','Novembre','Décembre'];
    return `${date.getDate()} ${months[date.getMonth()]} ${date.getFullYear()}`;
  }
}

function ymd(date) { return date.toISOString().split('T')[0]; }

// Normalise un titre pour matching cross-source (Allociné, JustWatch)
function normalizeTitle(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // accents
    .replace(/[''`]/g, "'")
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ─── CACHE GÉNÉRIQUE ──────────────────────────────────────────────────────────

function loadCache(filePath) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (parsed.__version !== CACHE_VERSION) return { __version: CACHE_VERSION };
    return parsed;
  } catch {
    return { __version: CACHE_VERSION };
  }
}

function saveCache(filePath, cache) {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    cache.__version = CACHE_VERSION;
    const tmp = filePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, filePath);
  } catch (err) {
    console.warn(`  ⚠️  Cache ${path.basename(filePath)} non sauvegardé : ${err.message}`);
  }
}

function isFresh(entry, ttlHours) {
  if (!entry?._cachedAt) return false;
  return (Date.now() - entry._cachedAt) / 3600000 < ttlHours;
}

// ─── HELPERS MÉTIER ───────────────────────────────────────────────────────────

function isFrenchProduction(details) {
  const hasFRCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin  = details.origin_country?.includes('FR') ?? false;
  const isFrLang     = details.original_language === 'fr';
  // v7: assouplissement — 1 signal fort suffit si pas de signal contraire US/UK
  const hasUSCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'US') ?? false;
  const hasUKCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'GB') ?? false;
  const score = (hasFRCountry ? 1 : 0) + (hasFROrigin ? 1 : 0) + (isFrLang ? 1 : 0);
  if (score >= 2) return true;
  // Cas tolérant : original_language=fr + pas de country US/UK → probablement FR
  if (isFrLang && !hasUSCountry && !hasUKCountry) return true;
  return false;
}

function hasExcludedGenre(d)   { return d.genres?.some((g) => EXCLUDED_GENRE_IDS.has(g.id)) ?? false; }
function isSpectacle(d) {
  const titles = [d.title, d.original_title].filter(Boolean);
  const titlesLower = titles.map((t) => t.toLowerCase());
  // 1. Mots-clés substring
  if (titlesLower.some((t) => SPECTACLE_KEYWORDS.some((kw) => t.includes(kw)))) return true;
  // 2. v7.1 : Patterns regex sur le titre original
  if (titles.some((t) => SPECTACLE_TITLE_PATTERNS.some((rx) => rx.test(t)))) return true;
  // 3. v7.1 : Distributeur de spectacle vivant connu
  const distributor = (d.production_companies || []).find((c) => SPECTACLE_DISTRIBUTORS.has(c.name));
  if (distributor) return true;
  return false;
}
function isTelefilmByTitle(d) {
  return TELEFILM_TITLE_PATTERNS.some((rx) => rx.test(d.title || ''));
}
function isGhostEntry(d) {
  const noGenres = !d.genres || d.genres.length === 0;
  const noVotes  = (d.vote_count ?? 0) === 0 && (d.vote_average ?? 0) === 0;
  const noPoster = !d.poster_path;
  const badTitle = /^untitled$|^sans titre$|^\s*$/i.test(d.title || '');
  const score = (noGenres ? 1 : 0) + (noVotes ? 1 : 0) + (noPoster ? 1 : 0) + (badTitle ? 2 : 0);
  return score >= 2;
}
function isTooShort(d) {
  if (!d.runtime || d.runtime === 0) return false;
  return d.runtime < MIN_RUNTIME;
}
function hasTheatricalReleaseFR(rd) {
  const fr = rd?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return false;
  return fr.release_dates.some((x) => x.type === 3 || x.type === 2);
}
function getTheatricalDateFR(rd) {
  const fr = rd?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const t = fr.release_dates
    .filter((x) => x.type === 3 || x.type === 2)
    .map((x) => new Date(x.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return t ?? null;
}

// v7: ne fallback PAS sur US si FR absent (la date US est trompeuse pour Plex FR)
function getOfficialDigitalDateFR(rd) {
  const fr = rd?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const d = fr.release_dates
    .filter((x) => x.type === 4)
    .map((x) => new Date(x.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return d ? { date: d, region: 'FR' } : null;
}

// Extrait le distributeur principal (1ère production_company avec logo)
function getMainDistributor(details) {
  const companies = details.production_companies || [];
  // Priorité aux compagnies avec logo (= généralement les majors)
  const withLogo = companies.find((c) => c.logo_path);
  return (withLogo || companies[0])?.name || null;
}

// ─── APPRENTISSAGE DES DÉLAIS ─────────────────────────────────────────────────

function loadDelayHistory() {
  try {
    return JSON.parse(fs.readFileSync(DELAY_HISTORY, 'utf8'));
  } catch {
    return { records: [], learned: {} };
  }
}

function saveDelayHistory(history) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = DELAY_HISTORY + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(history, null, 2), 'utf8');
    fs.renameSync(tmp, DELAY_HISTORY);
  } catch (err) {
    console.warn(`  ⚠️  Historique délais non sauvegardé : ${err.message}`);
  }
}

// Calcule médiane (résiste aux outliers contrairement à la moyenne)
function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

// Calcule les délais appris à partir de l'historique
function computeLearnedDelays(history) {
  const byDistributor = {};
  for (const r of history.records || []) {
    if (!r.distributor || !r.observedDelayDays) continue;
    if (!byDistributor[r.distributor]) byDistributor[r.distributor] = [];
    byDistributor[r.distributor].push(r.observedDelayDays);
  }
  const learned = {};
  for (const [dist, delays] of Object.entries(byDistributor)) {
    if (delays.length >= LEARNING_MIN_SAMPLES) {
      learned[dist] = { days: Math.round(median(delays)), samples: delays.length };
    }
  }
  history.learned = learned;
  return learned;
}

// Renvoie le meilleur délai disponible pour un film
function getBestDelay(distributor, isFrench, learned) {
  // 1. Délai appris (priorité absolue si assez d'échantillons)
  if (distributor && learned[distributor]) {
    return { days: learned[distributor].days, source: `learned-${learned[distributor].samples}` };
  }
  // 2. Hint connu pour ce distributeur
  if (distributor && DISTRIBUTOR_HINTS[distributor]) {
    return { days: DISTRIBUTOR_HINTS[distributor].days, source: 'hint' };
  }
  // 3. Défaut par nationalité
  return {
    days: isFrench ? DEFAULT_DELAYS.FRENCH : DEFAULT_DELAYS.AMERICAN,
    source: 'default',
  };
}

// Enregistre une observation pour apprentissage futur
function recordObservation(history, { tmdbId, title, distributor, cinemaDate, vodDate, isFrench }) {
  const observedDelayDays = Math.round((vodDate - cinemaDate) / 86400000);
  if (observedDelayDays < 5 || observedDelayDays > 365) return; // outlier évident
  // Évite doublons (même tmdb_id)
  history.records = (history.records || []).filter((r) => r.tmdbId !== tmdbId);
  history.records.push({
    tmdbId,
    title,
    distributor,
    isFrench,
    cinemaDate: ymd(cinemaDate),
    vodDate: ymd(vodDate),
    observedDelayDays,
    recordedAt: ymd(new Date()),
  });
  // Garde les 500 derniers records (rolling window)
  if (history.records.length > 500) {
    history.records = history.records.slice(-500);
  }
}

// ─── JUSTWATCH (API non-officielle GraphQL) ──────────────────────────────────

const JW_GRAPHQL_URL = 'https://apis.justwatch.com/graphql';
const JW_QUERY = `
  query GetSearchTitles($searchTitlesFilter: TitleFilter!, $country: Country!, $language: Language!) {
    popularTitles(country: $country, filter: $searchTitlesFilter, first: 5) {
      edges {
        node {
          objectId objectType
          content(country: $country, language: $language) {
            title originalReleaseYear
          }
          offers(country: $country, platform: WEB) {
            monetizationType
            availableTo
            availableFromTime
            package { clearName }
          }
        }
      }
    }
  }
`;

async function lookupJustWatchCountry(title, year, country, language, cache) {
  const key = `${country}::${normalizeTitle(title)}::${year || ''}`;
  if (cache[key] && isFresh(cache[key], JW_TTL_H)) return cache[key].data;

  try {
    const body = {
      operationName: 'GetSearchTitles',
      query: JW_QUERY,
      variables: {
        searchTitlesFilter: { searchQuery: title },
        country,
        language,
      },
    };
    const res = await fetchWithRetry(JW_GRAPHQL_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const edges = res?.data?.popularTitles?.edges || [];
    const targetTitle = normalizeTitle(title);
    let best = edges.find((e) => {
      const t = normalizeTitle(e.node.content?.title);
      const y = e.node.content?.originalReleaseYear;
      return t === targetTitle && (!year || Math.abs(y - year) <= 1);
    });
    if (!best && edges.length) {
      best = edges.find((e) => normalizeTitle(e.node.content?.title) === targetTitle);
    }
    if (!best) {
      cache[key] = { _cachedAt: Date.now(), data: null };
      return null;
    }
    const monetized = (best.node.offers || []).filter(
      (o) => ['BUY', 'RENT'].includes(o.monetizationType) && o.availableFromTime
    );
    if (!monetized.length) {
      cache[key] = { _cachedAt: Date.now(), data: null };
      return null;
    }
    const earliest = monetized
      .map((o) => new Date(o.availableFromTime))
      .filter((d) => !isNaN(d))
      .sort((a, b) => a - b)[0];
    const data = earliest ? { date: earliest.toISOString(), country } : null;
    cache[key] = { _cachedAt: Date.now(), data };
    return data;
  } catch (err) {
    return null;
  }
}

// v7.1 : Cascade FR → CA → US (CA/US capture les VFQ et sorties US TVOD précoces)
async function lookupJustWatch(title, year, cache) {
  const fr = await lookupJustWatchCountry(title, year, 'FR', 'fr', cache);
  if (fr) return { ...fr, region: 'fr' };
  const ca = await lookupJustWatchCountry(title, year, 'CA', 'fr', cache);
  if (ca) return { ...ca, region: 'na' };
  const us = await lookupJustWatchCountry(title, year, 'US', 'en', cache);
  if (us) return { ...us, region: 'na' };
  return null;
}

// ─── ALLOCINÉ (scraping HTML léger) ──────────────────────────────────────────

// Allociné publie une page "Sorties VOD/SVOD de la semaine" très stable.
// Format : https://www.allocine.fr/film/agenda/sem-YYYY-MM-DD/
// Structure HTML : items avec titre + date de sortie VOD.
async function fetchAllocineWeek(weekStart, cache) {
  const dateStr = ymd(weekStart);
  const key = `week-${dateStr}`;
  if (cache[key] && isFresh(cache[key], ALLO_TTL_H)) return cache[key].data;

  const url = `https://www.allocine.fr/film/agenda/sem-${dateStr}/`;
  try {
    const html = await fetchWithRetry(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; CinocheFR-VODBot/7.0; respectful)',
        'Accept': 'text/html,application/xhtml+xml',
      },
    });
    if (typeof html !== 'string') {
      cache[key] = { _cachedAt: Date.now(), data: [] };
      return [];
    }
    // Parse tolérant : extrait titres des cartes de film "VOD"
    const items = [];
    // Pattern pour extraire les blocs de films (Allociné utilise data-attributes stables)
    const cardRegex = /<div[^>]*class="[^"]*card entity-card[^"]*"[^>]*>([\s\S]*?)<\/div>\s*<\/div>\s*<\/div>/g;
    const titleRegex = /<a[^>]*class="[^"]*meta-title-link[^"]*"[^>]*>([^<]+)<\/a>/;
    let m;
    while ((m = cardRegex.exec(html)) !== null) {
      const block = m[1];
      const tm = titleRegex.exec(block);
      if (tm) {
        items.push({
          title: tm[1].trim().replace(/&#039;/g, "'").replace(/&amp;/g, '&'),
          weekOf: dateStr,
        });
      }
    }
    cache[key] = { _cachedAt: Date.now(), data: items };
    return items;
  } catch (err) {
    cache[key] = { _cachedAt: Date.now(), data: [] };
    return [];
  }
}

// Récupère les sorties VOD Allociné sur une fenêtre temporelle
async function buildAllocineIndex(windowStart, windowEnd, cache) {
  const index = new Map(); // normalized_title → date
  // Allociné = 1 page par semaine. On scanne semaine par semaine.
  const cursor = new Date(windowStart);
  cursor.setDate(cursor.getDate() - cursor.getDay() + 1); // recule au lundi
  while (cursor <= windowEnd) {
    const items = await fetchAllocineWeek(new Date(cursor), cache);
    for (const it of items) {
      const norm = normalizeTitle(it.title);
      if (!index.has(norm)) {
        index.set(norm, new Date(it.weekOf));
      }
    }
    cursor.setDate(cursor.getDate() + 7);
    await sleep(ALLO_DELAY_MS);
  }
  return index;
}

// ─── PIPELINE ENDPOINTS TMDB ──────────────────────────────────────────────────

function computeWindow(now = new Date()) {
  const start = new Date(now); start.setDate(start.getDate() - WINDOW_DAYS_BEFORE); start.setHours(0,0,0,0);
  const end   = new Date(now); end.setDate(end.getDate() + WINDOW_DAYS_AFTER);    end.setHours(23,59,59,999);
  return { windowStart: start, windowEnd: end };
}

function buildScanEndpoints(windowStart, windowEnd) {
  // FR : films sortis entre il y a 7 mois et il y a 3 mois
  const frStart = new Date(windowEnd); frStart.setMonth(frStart.getMonth() - 7);
  const frEnd   = new Date(windowStart); frEnd.setDate(frEnd.getDate() - 90);
  // INTL : films sortis entre il y a 4 mois et il y a 2 semaines (PVOD rapide possible)
  const intlStart = new Date(windowEnd); intlStart.setMonth(intlStart.getMonth() - 5);
  const intlEnd   = new Date(windowStart); intlEnd.setDate(intlEnd.getDate() - 14);

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

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isFresh(cache[key], TMDB_TTL_H)) return cache[key].data;
  const url = `https://api.themoviedb.org/3/movie/${movieId}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`;
  const data = await fetchWithRetry(url);
  cache[key] = { _cachedAt: Date.now(), data };
  return data;
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v7 — démarrage...\n');

  const now = new Date();
  const { windowStart, windowEnd } = computeWindow(now);
  console.log(`📅  Fenêtre glissante : ${formatDateFR(windowStart)} → ${formatDateFR(windowEnd)} (${WINDOW_DAYS_BEFORE}j avant, ${WINDOW_DAYS_AFTER}j après)\n`);

  const tmdbCache = loadCache(TMDB_CACHE);
  const jwCache   = loadCache(JW_CACHE);
  const alloCache = loadCache(ALLO_CACHE);
  const history   = loadDelayHistory();
  const learned   = computeLearnedDelays(history);

  console.log(`💾  Caches : TMDB ${Object.keys(tmdbCache).length-1} | JW ${Object.keys(jwCache).length-1} | Allo ${Object.keys(alloCache).length-1}`);
  console.log(`🧠  Apprentissage : ${Object.keys(learned).length} distributeurs avec délais appris\n`);

  // ── Étape 1 : Index Allociné (rapide, indispensable pour cross-check) ──
  console.log('  🔎  Construction index Allociné...');
  let alloIndex;
  try {
    alloIndex = await buildAllocineIndex(windowStart, windowEnd, alloCache);
    console.log(`  ✓ ${alloIndex.size} entrées Allociné indexées\n`);
  } catch (err) {
    console.warn(`  ⚠️  Allociné indisponible : ${err.message}\n`);
    alloIndex = new Map();
  }
  saveCache(ALLO_CACHE, alloCache);

  // ── Étape 2 : Scan TMDB ──
  const endpoints = buildScanEndpoints(windowStart, windowEnd);
  const rawMovies = [];
  console.log('  🔎  Scan TMDB :');
  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`     [${String(i+1).padStart(2,'0')}/${endpoints.length}] ${name.padEnd(22)} `);
    try {
      const results = await fetchAllPages(url);
      rawMovies.push(...results);
      console.log(`→ ${results.length} films`);
    } catch (err) {
      console.log(`⚠️  ${err.message}`);
    }
    await sleep(API_DELAY_MS);
  }
  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  console.log(`\n  📦  ${uniqueMovies.length} films uniques TMDB\n`);

  // ── Étape 3 : Analyse détaillée + résolution date VOD ──
  console.log('  🔬  Résolution dates VOD :');
  const finalResults = [];
  const drops = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0,
    spectacle: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, beforeWindow: 0, afterWindow: 0,
  };
  let cacheHits = 0;

  for (const movie of uniqueMovies) {
    const cachedBefore = tmdbCache[String(movie.id)] && isFresh(tmdbCache[String(movie.id)], TMDB_TTL_H);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

    let details;
    try { details = await fetchMovieDetails(movie.id, tmdbCache); }
    catch { drops.noDetails++; continue; }

    if (!details.release_date)        { drops.noReleaseDate++;   continue; }
    if (hasExcludedGenre(details))    { drops.excludedGenre++;   continue; }
    if (isTelefilmByTitle(details))   { drops.telefilmByTitle++; continue; }
    if (isSpectacle(details))         { drops.spectacle++;       continue; }
    if (isTooShort(details))          { drops.tooShort++;        continue; }
    if (isGhostEntry(details))        { drops.ghostEntry++;      continue; }
    if (!details.genres?.length)      { drops.noGenresAtAll++;   continue; }

    const isFrench  = isFrenchProduction(details);
    const hasFRCine = hasTheatricalReleaseFR(details.release_dates);
    if (!hasFRCine) { drops.noFRTheatrical++; continue; }

    // ── Filtre qualité v7.1 (STRICT : virer fantômes, garder cinéma d'auteur réel) ──
    const voteCount  = details.vote_count ?? 0;
    const voteAvg    = details.vote_average ?? 0;
    const popularity = movie.popularity ?? details.popularity ?? 0;
    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { drops.noReleaseDate++; continue; }
    const ageDays = (now - cinemaDate) / 86400000;

    // Règle absolue : aucun vote ET aucune note = fantôme/captation → drop
    // (un vrai film de cinéma sorti en salle a TOUJOURS au moins 1-2 votes TMDB)
    if (voteCount === 0 && voteAvg === 0) {
      drops.lowQuality++; continue;
    }

    const isSafeVolume   = voteCount >= 5;
    const isNicheButReal = popularity >= 1.5;
    // Bypass FR récent retiré en v7.1 : sauvait trop de captations (cf. Booder)
    if (!isSafeVolume && !isNicheButReal) {
      drops.lowQuality++; continue;
    }

    // ── Résolution date VOD multi-sources ──
    const distributor = getMainDistributor(details);
    const officialDigital = getOfficialDigitalDateFR(details.release_dates); // FR-only
    const year = cinemaDate.getFullYear();

    // Tentative JustWatch (sans bloquer si échec)
    let jwData = null;
    try { jwData = await lookupJustWatch(details.title, year, jwCache); }
    catch {}

    // Tentative Allociné (lookup dans l'index pré-calculé)
    const alloDate = alloIndex.get(normalizeTitle(details.title))
                   || alloIndex.get(normalizeTitle(details.original_title));

    // ── Décision finale : choisir la meilleure source ──
    // Ordre de priorité : Allociné > TMDB officielle FR > JustWatch > Prédiction
    let vodDate, source, confidence;
    const delayInfo = getBestDelay(distributor, isFrench, learned);

    if (alloDate) {
      vodDate = alloDate;
      source = 'allocine';
      confidence = 0.95;
    } else if (officialDigital) {
      vodDate = officialDigital.date;
      source = 'tmdb-official-fr';
      confidence = 0.90;
    } else if (jwData?.date) {
      vodDate = new Date(jwData.date);
      // v7.1 : distingue FR (haute confiance) vs NA = US/CA pour VFQ (confiance moyenne)
      if (jwData.region === 'fr') {
        source = 'justwatch-fr';
        confidence = 0.85;
      } else {
        source = 'justwatch-na';
        confidence = 0.75;
      }
    } else {
      // Prédiction avec délai appris/hint/défaut
      const vod = new Date(cinemaDate);
      vod.setDate(vod.getDate() + delayInfo.days);
      vodDate = vod;
      source = `predicted-${delayInfo.source}`;
      confidence = delayInfo.source === 'learned' ? 0.70
                 : delayInfo.source === 'hint'    ? 0.55
                 : 0.40;
    }

    if (vodDate < windowStart) { drops.beforeWindow++; continue; }
    if (vodDate > windowEnd)   { drops.afterWindow++;  continue; }

    // Enregistre l'observation pour apprentissage si source fiable
    if (['allocine', 'tmdb-official-fr', 'justwatch-fr', 'justwatch-na'].includes(source) && distributor) {
      recordObservation(history, {
        tmdbId: movie.id,
        title: details.title,
        distributor,
        cinemaDate,
        vodDate,
        isFrench,
      });
    }

    finalResults.push({
      // ── Champs requis par index.html (CONTRAT FRONT) ──
      title       : details.title || movie.title,
      plex_release: formatDateFR(vodDate),
      tmdb_id     : movie.id,
      poster_path : details.poster_path || movie.poster_path,
      // ── Champs bonus pour debug et futur enrichissement UI ──
      original_title: details.original_title,
      cinema_date   : formatDateFR(cinemaDate),
      vote_average  : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count    : voteCount,
      genres        : details.genres.map((g) => g.name),
      is_french     : isFrench,
      distributor,
      source,
      confidence,
      _sortDate     : vodDate.getTime(),
      _popularity   : popularity,
    });

    const flag = isFrench ? '🇫🇷' : '🌍';
    const srcEmoji = source === 'allocine' ? '🅰️ '
                   : source === 'tmdb-official-fr' ? '✓ '
                   : source === 'justwatch-fr' ? '📺'
                   : source === 'justwatch-na' ? '🇨🇦'
                   : '~ ';
    // v7.1 : warning visible si confidence basse (films à risque)
    const warn = confidence < 0.5 ? ' ⚠️ ' : '   ';
    console.log(`     ${flag} ${srcEmoji}${warn}${(details.title || '').padEnd(38).slice(0,38)} → ${formatDateFR(vodDate)} (conf: ${confidence.toFixed(2)})`);
  }

  // ── Étape 4 : Tri, dédup, écriture ──
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });
  const deduped = Array.from(new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values());
  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  fs.mkdirSync(DATA_DIR, { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  // ── Étape 5 : Persist caches & history ──
  const cutoff7d = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(tmdbCache)) {
    if (k === '__version') continue;
    if (!tmdbCache[k]?._cachedAt || tmdbCache[k]._cachedAt < cutoff7d) delete tmdbCache[k];
  }
  saveCache(TMDB_CACHE, tmdbCache);
  saveCache(JW_CACHE, jwCache);
  saveDelayHistory(history);

  // ── Récap ──
  const frCount = output.filter((m) => m.is_french).length;
  const sourceBreakdown = output.reduce((acc, m) => {
    acc[m.source] = (acc[m.source] || 0) + 1;
    return acc;
  }, {});
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`\n  ✅ Terminé en ${elapsed}s : ${output.length} films générés (${frCount} 🇫🇷, ${output.length - frCount} 🌍)`);
  console.log(`  📊 Sources : ${Object.entries(sourceBreakdown).map(([k,v]) => `${k}=${v}`).join(', ')}`);
  console.log(`  🗑️  Drops : ${Object.entries(drops).filter(([,v]) => v > 0).map(([k,v]) => `${k}=${v}`).join(', ')}`);
  console.log(`  💾 Cache hits TMDB : ${cacheHits}/${uniqueMovies.length} (${Math.round(100*cacheHits/uniqueMovies.length)}%)`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
