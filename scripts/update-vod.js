/**
 * updateVOD.js — v6.1
 * ===================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle.
 *
 * BASE : v6 (structure conservée à l'identique)
 *
 * FIXES v6.1 (chirurgicaux, sans surengineering) :
 *
 * 🔧 FIX 1 — Bug Marsupilami : suppression du fallback date digitale US.
 *            getOfficialDigitalDateFR() ne cherche plus qu'en FR (type=4).
 *            Le fallback US renvoyait des dates placeholder/pre-order qui
 *            faisaient apparaître des films encore en salle.
 *
 * 🔧 FIX 2 — Garde-fou délai minimum sur dates officielles aussi :
 *            le check actualDelayDays < minDelay s'applique maintenant
 *            à TOUTES les sources (officielle et prédite).
 *
 * 🔧 FIX 3 — looksLikeCaptation() : filtre les stand-up/spectacles mal
 *            catalogués (0 vote, 0 note, genre unique "Comédie").
 *            Exemption totale pour les productions francophones (FR/BE/CH).
 *            → récupère Animal Totem, Louise, Les Baronnes
 *
 * 🔧 FIX 4 — isSpectacle() : retrait de 'théâtre'/'theatre' des keywords
 *            (trop générique, faux positifs sur films légitimes).
 *            Ajout de SPECTACLE_TITLE_PATTERNS (regex) et
 *            SPECTACLE_DISTRIBUTORS (blacklist distributeurs captation).
 *            Guard : jamais killer un film francophone avec runtime > 60 min
 *            sauf via distributeur blacklisté.
 *
 * 🔧 FIX 5 — Endpoints BE/CH francophones ajoutés au scan TMDB.
 *            → Les Baronnes et films belges/suisses en français sont couverts.
 *
 * 🔧 FIX 6 — hasTheatricalReleaseFR() accepte type=1 (premiere) en plus
 *            de type=2/3 pour les films francophones hors France.
 *
 * 🔧 FIX 7 — Tolérance de 7 jours après windowEnd (toleratedEnd).
 *            → À la poursuite du Père Noël ! et films de fin de mois.
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * * (tous les soirs à 2h)
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH    = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH   = path.join(__dirname, '../data/.tmdb-cache.json');

const DELAYS = {
  FRENCH  : 120,   // Chronologie des médias FR — VOD à l'acte
  AMERICAN:  45,   // PVOD/TVOD international standard
};

// Cache TMDB : durée de validité d'une entrée détails (en heures)
const CACHE_TTL_HOURS = 24;
// Délai entre appels TMDB (limite officielle : ~50 req/s, on reste très conservateur)
const API_DELAY_MS = 130;
// Pages max par endpoint discover
const MAX_PAGES_PER_ENDPOINT = 6;
// Durée minimum pour ne pas être un court-métrage (minutes)
const MIN_RUNTIME = 40;

// FIX 7 : tolérance après la fin du mois pour capturer les VOD de fin de période
const WINDOW_END_TOLERANCE_DAYS = 7;

// Genres TMDB exclus : Documentaire (99), Musique (10402), Téléfilm (10770)
const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]);

/**
 * Mots-clés titres qui trahissent une captation de spectacle vivant.
 * FIX 4 : 'théâtre' et 'theatre' RETIRÉS (trop génériques, faux positifs).
 */
const SPECTACLE_KEYWORDS = [
  'symphony', 'symphonie', 'philharmonic', 'philharmonique',
  ' opera', 'opera:', "l'opéra", 'opéra de paris', 'paris opera',
  'in concert', 'live at', 'live in', 'live from', 'en concert',
  ' tour ', 'world tour', 'la tournée',
  ' ballet', 'casse-noisette', 'nutcracker', 'der nussknacker',
  'récital', 'recital', 'metropolitan opera', 'royal opera',
  'gaming x symphony',
  'pièce de', 'comédie française', 'captation',
  'molière', 'charbon dans les veines',
  // NOTE : 'théâtre'/'theatre' intentionnellement absents (FIX 4)
];

/**
 * FIX 4 : Patterns regex sur les titres — captations stand-up / spectacle.
 */
const SPECTACLE_TITLE_PATTERNS = [
  /fête ses?\s+\d+\s+ans/i,
  /\bau\s+(zénith|olympia|casino de paris|grand rex|palais des sports)\b/i,
  /\bsur scène\b/i,
  /\bone\s*[- ]?\s*(wo)?man\s*[- ]?\s*show\b/i,
];

/**
 * FIX 4 : Distributeurs spectacle vivant connus → drop direct.
 */
const SPECTACLE_DISTRIBUTORS = new Set([
  'Arpagon Productions',
  'Pascal Légitimus Productions',
  'JMD Production',
  "Ki M'aime Me Suive",
  'Productions du Théâtre',
  'Comédie Française',
]);

/**
 * Patterns qui trahissent un téléfilm policier régional FR.
 */
const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

// ─── HELPERS DE BASE ───────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

function formatDateFR(date) {
  try {
    return date.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' });
  } catch {
    const months = ['Janvier','Février','Mars','Avril','Mai','Juin',
                    'Juillet','Août','Septembre','Octobre','Novembre','Décembre'];
    return `${date.getDate()} ${months[date.getMonth()]} ${date.getFullYear()}`;
  }
}

function ymd(date) { return date.toISOString().split('T')[0]; }

// ─── CACHE TMDB ────────────────────────────────────────────────────────────────

function loadCache() {
  try { return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8')); }
  catch { return {}; }
}

function saveCache(cache) {
  try {
    fs.mkdirSync(path.dirname(CACHE_PATH), { recursive: true });
    const tmp = CACHE_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, CACHE_PATH);
  } catch (err) {
    console.warn(`  ⚠️  Cache non sauvegardé : ${err.message}`);
  }
}

function isCacheEntryFresh(entry) {
  if (!entry?._cachedAt) return false;
  return (Date.now() - entry._cachedAt) / 3600000 < CACHE_TTL_HOURS;
}

// ─── HELPERS MÉTIER ────────────────────────────────────────────────────────────

function isFrenchProduction(details) {
  const hasFRCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin  = details.origin_country?.includes('FR') ?? false;
  const isFrLang     = details.original_language === 'fr';
  const score = (hasFRCountry ? 1 : 0) + (hasFROrigin ? 1 : 0) + (isFrLang ? 1 : 0);
  return score >= 2;
}

/**
 * FIX 3 & 4 : Productions francophones au sens large (FR, BE, CH, LU…).
 * Utilisé comme garde-fou dans looksLikeCaptation() et isSpectacle().
 */
function isFrancophoneProduction(details) {
  if (details.original_language === 'fr') return true;
  const francoCountries = new Set(['FR', 'BE', 'CH', 'LU', 'CA', 'MA', 'SN', 'CI']);
  return details.production_countries?.some((c) => francoCountries.has(c.iso_3166_1)) ?? false;
}

function hasExcludedGenre(details) {
  return details.genres?.some((g) => EXCLUDED_GENRE_IDS.has(g.id)) ?? false;
}

/**
 * FIX 4 : Détecte les captations de spectacle vivant.
 * - Keywords et patterns : cherchent uniquement dans les TITRES.
 * - Guard : un film francophone avec runtime > 60 min ne peut être éliminé
 *   que par un distributeur blacklisté (jamais par keyword/pattern).
 */
function isSpectacle(details) {
  const isFrancophone = isFrancophoneProduction(details);
  const hasRuntime    = (details.runtime ?? 0) > 60;

  // Distributeur blacklisté → drop direct, même pour les films FR
  if ((details.production_companies || []).some((c) => SPECTACLE_DISTRIBUTORS.has(c.name))) {
    return true;
  }

  // Guard : film francophone long → pas une captation via keyword/pattern
  if (isFrancophone && hasRuntime) return false;

  // Scan keywords sur les titres uniquement (pas l'overview)
  const titlesLower = [details.title, details.original_title]
    .filter(Boolean)
    .map((t) => t.toLowerCase());
  if (titlesLower.some((t) => SPECTACLE_KEYWORDS.some((kw) => t.includes(kw)))) return true;

  // Patterns regex sur les titres
  const titles = [details.title, details.original_title].filter(Boolean);
  if (titles.some((t) => SPECTACLE_TITLE_PATTERNS.some((rx) => rx.test(t)))) return true;

  return false;
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
  if (!details.runtime || details.runtime === 0) return false;
  return details.runtime < MIN_RUNTIME;
}

/**
 * FIX 3 : Détecte une signature de captation de spectacle mal catalogué :
 * 0 vote + 0 note + genre unique "Comédie".
 * Exemption complète pour les productions francophones
 * (Animal Totem, Louise, Les Baronnes sont des films FR/BE légitimes).
 */
function looksLikeCaptation(details) {
  if (isFrancophoneProduction(details)) return false; // exemption FR/BE/CH
  const noVotes     = (details.vote_count ?? 0) === 0 && (details.vote_average ?? 0) === 0;
  const onlyComedie = details.genres?.length === 1 && details.genres[0].name === 'Comédie';
  return noVotes && onlyComedie;
}

/**
 * FIX 6 : Accepte type=1 (premiere) en plus de type=2/3 (theatrical)
 * pour couvrir les films BE/CH qui n'ont parfois pas type=3 en FR sur TMDB.
 */
function hasTheatricalReleaseFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return false;
  return fr.release_dates.some((rd) => rd.type === 1 || rd.type === 2 || rd.type === 3);
}

function getTheatricalDateFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const theatrical = fr.release_dates
    .filter((rd) => rd.type === 1 || rd.type === 2 || rd.type === 3)
    .map((rd) => new Date(rd.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return theatrical ?? null;
}

/**
 * FIX 1 : Ne cherche la date digitale officielle QU'EN FR (type=4).
 * Suppression du fallback US qui renvoyait des dates placeholder
 * (pre-order Amazon, Prime Video) faisant apparaître des films encore en salle.
 *
 * AVANT (v6) : cherchait FR puis US en fallback → Marsupilami (sorti le 4/02)
 *              apparaissait car Prime Video US avait une date type=4 fictive
 *              dans la fenêtre du mois en cours.
 * APRÈS : uniquement type=4 FR → si pas de date officielle FR, on prédit.
 */
function getOfficialDigitalDateFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const digital = fr.release_dates
    .filter((rd) => rd.type === 4)
    .map((rd) => new Date(rd.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return digital ? { date: digital, region: 'FR' } : null;
}

function predictVODDate(cinemaDate, isFrench) {
  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + (isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN));
  return vod;
}

// ─── FENÊTRE CIBLE ─────────────────────────────────────────────────────────────

/**
 * Mois strictement en cours, aucune anticipation sur le mois suivant.
 * FIX 7 : toleratedEnd = windowEnd + 7j pour les VOD de fin de mois.
 */
function computeTargetWindow(now = new Date()) {
  const monthStart   = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd     = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
  const toleratedEnd = new Date(monthEnd);
  toleratedEnd.setDate(toleratedEnd.getDate() + WINDOW_END_TOLERANCE_DAYS);
  return { monthStart, monthEnd, toleratedEnd };
}

// ─── ENDPOINTS TMDB ───────────────────────────────────────────────────────────

function buildScanEndpoints(monthStart, monthEnd) {
  // Fenêtre scan FR : films sortis entre 6,5 mois et 100 jours avant monthEnd
  const frStart = new Date(monthEnd);
  frStart.setMonth(frStart.getMonth() - 6);
  frStart.setDate(frStart.getDate() - 15);
  const frEnd = new Date(monthStart);
  frEnd.setDate(frEnd.getDate() - 100);

  // Fenêtre scan INTL : films sortis entre 4 mois et 30 jours avant monthEnd
  const intlStart = new Date(monthEnd);
  intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd = new Date(monthStart);
  intlEnd.setDate(intlEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false`;

  return [
    // ── Films français ──────────────────────────────────────────────────────
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
    // ── FIX 5 : Films BE et CH francophones (ex: Les Baronnes) ──────────────
    { name: 'BE/franco/recent',
      url: `${base}&with_origin_country=BE&with_original_language=fr&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },
    { name: 'CH/franco/recent',
      url: `${base}&with_origin_country=CH&with_original_language=fr&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },
    // ── Films internationaux ─────────────────────────────────────────────────
    { name: 'INTL/popular',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/quality',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=vote_average.desc&vote_count.gte=40` },
    { name: 'INTL/fresh',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=primary_release_date.desc` },
  ];
}

// ─── DÉTAILS FILM (avec cache) ────────────────────────────────────────────────

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isCacheEntryFresh(cache[key])) return cache[key].data;
  const url = `https://api.themoviedb.org/3/movie/${movieId}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`;
  const data = await fetchWithRetry(url);
  cache[key] = { _cachedAt: Date.now(), data };
  return data;
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v6.1 — démarrage...\n');

  const now = new Date();
  const { monthStart, monthEnd, toleratedEnd } = computeTargetWindow(now);

  console.log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}`);
  console.log(`📅  Tolérance fin de mois : +${WINDOW_END_TOLERANCE_DAYS}j → ${formatDateFR(toleratedEnd)}\n`);

  const cache = loadCache();
  console.log(`💾  Cache : ${Object.keys(cache).length} entrées chargées\n`);

  // Étape 1 : Scan TMDB
  const endpoints = buildScanEndpoints(monthStart, monthEnd);
  const rawMovies = [];
  console.log('  🔎  Scan TMDB :');
  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`     [${String(i + 1).padStart(2, '0')}/${endpoints.length}] ${name.padEnd(22)} `);
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
  console.log(`\n  📦  ${uniqueMovies.length} films uniques\n`);

  // Étape 2 : Analyse
  const finalResults = [];
  const drops = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0,
    spectacle: 0, captation: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, delayTooShort: 0, beforeWindow: 0, afterWindow: 0,
  };
  let cacheHits = 0;

  console.log('  🔬  Analyse détaillée :');
  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

    let details;
    try { details = await fetchMovieDetails(movie.id, cache); }
    catch { drops.noDetails++; continue; }

    if (!details.release_date)        { drops.noReleaseDate++;   continue; }
    if (hasExcludedGenre(details))    { drops.excludedGenre++;   continue; }
    if (isTelefilmByTitle(details))   { drops.telefilmByTitle++; continue; }
    if (isSpectacle(details))         { drops.spectacle++;       continue; } // FIX 4
    if (looksLikeCaptation(details))  { drops.captation++;       continue; } // FIX 3
    if (isTooShort(details))          { drops.tooShort++;        continue; }
    if (isGhostEntry(details))        { drops.ghostEntry++;      continue; }
    if (!details.genres?.length)      { drops.noGenresAtAll++;   continue; }

    const isFrench  = isFrenchProduction(details);
    const minDelay  = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine = hasTheatricalReleaseFR(details.release_dates); // FIX 6

    if (!hasFRCine) { drops.noFRTheatrical++; continue; }

    // Filtre qualité composite (conservé de v6)
    const voteCount  = details.vote_count ?? 0;
    const popularity = movie.popularity ?? details.popularity ?? 0;
    if (voteCount < 5 && popularity < 1.5) { drops.lowQuality++; continue; }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { drops.noReleaseDate++; continue; }

    // FIX 1 : date officielle FR uniquement (plus de fallback US)
    const officialDigital = getOfficialDigitalDateFR(details.release_dates);
    const predictedDate   = predictVODDate(cinemaDate, isFrench);

    let vodDate, source;
    if (officialDigital) {
      vodDate = officialDigital.date;
      source  = 'officielle-fr';
    } else {
      vodDate = predictedDate;
      source  = 'prédite';
    }

    // FIX 2 : délai minimum appliqué sur toutes les sources (pas seulement prédite)
    const actualDelayDays = (vodDate - cinemaDate) / 86400000;
    if (actualDelayDays < minDelay) { drops.delayTooShort++; continue; }

    if (vodDate < monthStart) { drops.beforeWindow++; continue; }
    // FIX 7 : tolérance +7j sur la fin de fenêtre
    if (vodDate > toleratedEnd) { drops.afterWindow++; continue; }

    finalResults.push({
      title         : details.title || movie.title,
      plex_release  : formatDateFR(vodDate),
      tmdb_id       : movie.id,
      poster_path   : details.poster_path || movie.poster_path,
      original_title: details.original_title,
      cinema_date   : formatDateFR(cinemaDate),
      vote_average  : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count    : voteCount,
      genres        : details.genres.map((g) => g.name),
      is_french     : isFrench,
      source,
      _sortDate     : vodDate.getTime(),
      _popularity   : popularity,
    });

    const flag     = isFrench ? '🇫🇷' : '🌍';
    const srcShort = source === 'officielle-fr' ? '✓' : '~';
    console.log(`     ${flag} ${srcShort} ${(details.title || '').padEnd(45).slice(0, 45)} → ${formatDateFR(vodDate)}`);
  }

  // Étape 3 : Tri, dédup, écriture
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });
  const deduped = Array.from(
    new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values()
  );
  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  // Étape 4 : Nettoyage cache (entrées > 7 jours)
  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  // Récap
  const frCount       = output.filter((m) => m.is_french).length;
  const officielCount = output.filter((m) => m.source === 'officielle-fr').length;
  const elapsed       = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`\n  ✅ Terminé en ${elapsed}s : ${output.length} films générés (${frCount} 🇫🇷)`);
  console.log(`  📊 Sources : officielle-fr=${officielCount}, prédite=${output.length - officielCount}`);
  console.log(`  🗑️  Drops : ${Object.entries(drops).filter(([,v]) => v > 0).map(([k,v]) => `${k}=${v}`).join(', ')}`);
  console.log(`  💾 Cache hits : ${cacheHits}/${uniqueMovies.length}`);
  console.log(`\n  📋 Fixes actifs v6.1 :`);
  console.log(`     ✅ FIX1  date officielle FR uniquement (Marsupilami corrigé)`);
  console.log(`     ✅ FIX2  délai minimum garanti sur dates officielles aussi`);
  console.log(`     ✅ FIX3  looksLikeCaptation() + exemption francophones`);
  console.log(`     ✅ FIX4  isSpectacle() affiné (sans 'théâtre', guard FR long)`);
  console.log(`     ✅ FIX5  endpoints BE/CH francophones`);
  console.log(`     ✅ FIX6  hasTheatricalReleaseFR() accepte type=1`);
  console.log(`     ✅ FIX7  toleratedEnd = windowEnd +${WINDOW_END_TOLERANCE_DAYS}j`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
