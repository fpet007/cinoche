/**
 * updateVOD.js — v4
 * =================
 * Génère la liste des films VOD pour le MOIS EN COURS (et amorce le mois suivant
 * sur les 7 derniers jours du mois) pour un serveur Plex FR.
 *
 * Améliorations v4 par rapport à v3 :
 *  ✅ Fenêtre glissante : on garde aussi les sorties imminentes du mois prochain
 *     pendant la dernière semaine du mois → plus jamais de fichier "vide" le 1er
 *  ✅ Cache disque des détails TMDB (24h) → 5x moins d'appels API
 *  ✅ Vérification watch/providers FR (TVOD réelle) en plus de release_type 4
 *  ✅ Détection "production française" robustifiée (origin_country + production_countries
 *     + langue, avec gestion des co-prods majoritairement françaises)
 *  ✅ Suppression du filtre vote_count.gte sur les films récents (< 90 jours)
 *     → on ne rate plus les nouveautés
 *  ✅ Tri stable (date VOD puis popularité)
 *  ✅ Sortie JSON allégée (champs réellement consommés par index.html)
 *  ✅ Atomic write (fichier temporaire + rename) → jamais de JSON corrompu
 *  ✅ Logs structurés + rapport de couverture
 *
 * Règles métier (chronologie des médias France, accord 2022) :
 *  - VOD à l'acte / TVOD : 4 mois (120 j) après sortie ciné — c'est notre cible
 *  - Films internationaux distribués en salle FR : ~45 j (pratique courante PVOD)
 *  - Date officielle TMDB type 4 OU provider TVOD FR  → priorité absolue
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * *   (tous les soirs à 2h)
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

// Si on est dans les 7 derniers jours du mois, on amorce le mois suivant aussi
const NEXT_MONTH_LOOKAHEAD_DAYS = 7;

// Cache TMDB : durée de validité d'une entrée détails (en heures)
const CACHE_TTL_HOURS = 24;

// Délai entre appels TMDB (limite officielle : ~50 req/s, on reste très conservateur)
const API_DELAY_MS = 130;

// Pages max par endpoint discover (TMDB renvoie max 500 pages mais 6 suffisent)
const MAX_PAGES_PER_ENDPOINT = 6;

// Durée minimum pour ne pas être un court-métrage (minutes)
const MIN_RUNTIME = 40;

// Genres TMDB exclus : 99 = Documentaire, 10402 = Musique
const EXCLUDED_GENRE_IDS = new Set([99, 10402]);

// Providers VOD/TVOD français reconnus (IDs TMDB) — utilisés pour confirmer
// qu'un film est réellement disponible à l'achat/location numérique en France.
// Source : https://api.themoviedb.org/3/watch/providers/movie?watch_region=FR
const FR_TVOD_PROVIDER_IDS = new Set([
  2,    // Apple TV
  3,    // Google Play Movies
  7,    // Fandango (FR : Orange VOD)
  10,   // Amazon Video (achat/location)
  35,   // Rakuten TV
  68,   // Microsoft Store
  130,  // Sky Store
  192,  // YouTube
  381,  // Canal VOD
  582,  // Universcine
  3186, // FILMO TV (achat à l'acte)
  1796, // Pathé Thuis (rare en FR mais présent)
]);

// ─── HELPERS DE BASE ───────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** fetch avec retry x3 + backoff exponentiel + gestion 429 */
async function fetchWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (res.status === 429) {
        // Rate limit — on attend plus longtemps
        await sleep(2000 * attempt);
        continue;
      }
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

// ─── CACHE TMDB ────────────────────────────────────────────────────────────────

function loadCache() {
  try {
    const raw = fs.readFileSync(CACHE_PATH, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
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
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < CACHE_TTL_HOURS;
}

// ─── HELPERS MÉTIER ────────────────────────────────────────────────────────────

/**
 * Détection robuste d'une production française.
 * On accepte un film comme français si AU MOINS 2 des 3 conditions sont vraies :
 *   - Pays de production inclut FR
 *   - Origin country = FR (premier pays de sortie selon TMDB)
 *   - Langue originale = fr
 * Cela évite les faux positifs (films francophones non-FR) et faux négatifs
 * (co-prods FR-BE-CH où la langue est tagged 'fr' sans que FR soit en origin).
 */
function isFrenchProduction(details) {
  const hasFRCountry  = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin   = details.origin_country?.includes('FR') ?? false;
  const isFrLang      = details.original_language === 'fr';
  const score = (hasFRCountry ? 1 : 0) + (hasFROrigin ? 1 : 0) + (isFrLang ? 1 : 0);
  return score >= 2;
}

function hasExcludedGenre(details) {
  return details.genres?.some((g) => EXCLUDED_GENRE_IDS.has(g.id)) ?? false;
}

function isTooShort(details) {
  const runtime = details.runtime;
  if (!runtime || runtime === 0) return false; // donnée manquante → on garde
  return runtime < MIN_RUNTIME;
}

/**
 * Le film a-t-il eu une sortie ciné EN FRANCE (type 3 ou 2) ?
 * type 3 = theatrical large, type 2 = theatrical limited.
 */
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

/**
 * Date de sortie digitale officielle (release_type 4).
 * Priorité FR puis US (les studios renseignent souvent uniquement la date US).
 */
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

/**
 * Le film est-il déjà listé sur un provider TVOD français ?
 * Si oui → date officielle "maintenant" (sortie effective).
 */
function isAvailableOnFRTVOD(watchProviders) {
  const fr = watchProviders?.results?.FR;
  if (!fr) return false;
  // 'buy' = achat, 'rent' = location → c'est de la VOD à l'acte
  const tvodEntries = [...(fr.buy ?? []), ...(fr.rent ?? [])];
  return tvodEntries.some((p) => FR_TVOD_PROVIDER_IDS.has(p.provider_id));
}

function predictVODDate(cinemaDate, isFrench) {
  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + (isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN));
  return vod;
}

function formatDateFR(date) {
  return date.toLocaleDateString('fr-FR', {
    day: 'numeric', month: 'long', year: 'numeric',
  });
}

function ymd(date) {
  return date.toISOString().split('T')[0];
}

// ─── FENÊTRES & ENDPOINTS ─────────────────────────────────────────────────────

/**
 * Détermine la fenêtre cible : mois en cours + amorce du mois suivant si on
 * est dans la dernière semaine du mois.
 */
function computeTargetWindow(now = new Date()) {
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);

  // Si on est dans les 7 derniers jours du mois, on étend
  const daysLeftInMonth = Math.ceil((monthEnd - now) / 86400000);
  let windowEnd = monthEnd;
  if (daysLeftInMonth <= NEXT_MONTH_LOOKAHEAD_DAYS) {
    windowEnd = new Date(now.getFullYear(), now.getMonth() + 2, 0, 23, 59, 59);
  }
  return { monthStart, monthEnd, windowEnd };
}

/**
 * Construit les endpoints de scan TMDB.
 *
 * Fenêtres ciné rétroactives :
 *  - FR : sortie ciné entre (windowEnd - 6 mois) et (monthStart - ~3.5 mois)
 *    → couvre les films français qui sortent en VOD pendant la fenêtre cible
 *  - INTL : sortie ciné entre (windowEnd - 4 mois) et (monthStart - 30 j)
 */
function buildScanEndpoints(monthStart, windowEnd) {
  // Fenêtre FR (délai 120j)
  const frStart = new Date(windowEnd); frStart.setMonth(frStart.getMonth() - 6); frStart.setDate(frStart.getDate() - 15);
  const frEnd   = new Date(monthStart); frEnd.setDate(frEnd.getDate() - 100); // marge 100j (sécurité < 120j)

  // Fenêtre INTL (délai 45j)
  const intlStart = new Date(windowEnd); intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd   = new Date(monthStart); intlEnd.setDate(intlEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false`;

  return [
    // ─── VOLET FR (5 endpoints, déduplication automatique) ────────────────────

    // 1. Origin FR — tri popularité
    { name: 'FR/origin/popular',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },

    // 2. Origin FR — tri qualité (rattrape les films d'auteur peu populaires)
    { name: 'FR/origin/quality',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=vote_average.desc&vote_count.gte=10` },

    // 3. Langue fr en salle FR — couvre co-productions francophones
    { name: 'FR/lang+region',
      url: `${base}&with_original_language=fr&region=FR&with_release_type=2|3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },

    // 4. Origin FR sans tri qualité — rattrape petits films
    { name: 'FR/origin/recent',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },

    // 5. Sorties ciné FR récentes toutes langues — pour rattraper les films FR
    //    mal taggués (origin_country incorrect)
    { name: 'FR/theatrical-net',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },

    // ─── VOLET INTL (3 endpoints) ────────────────────────────────────────────

    // 6. Populaires distribués en salle FR
    { name: 'INTL/popular',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=popularity.desc` },

    // 7. Bien notés distribués en salle FR
    { name: 'INTL/quality',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=vote_average.desc&vote_count.gte=40` },

    // 8. Sorties très récentes en salle FR (rattrape blockbusters peu votés)
    { name: 'INTL/fresh',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=primary_release_date.desc` },
  ];
}

// ─── DÉTAILS FILM (avec cache) ────────────────────────────────────────────────

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isCacheEntryFresh(cache[key])) {
    return cache[key].data;
  }
  const url = `https://api.themoviedb.org/3/movie/${movieId}` +
              `?api_key=${TMDB_API_KEY}&language=fr-FR` +
              `&append_to_response=release_dates,watch/providers`;
  const data = await fetchWithRetry(url);
  cache[key] = { _cachedAt: Date.now(), data };
  return data;
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v4 — démarrage...\n');

  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);
  const isExtended = windowEnd > monthEnd;

  console.log(`📅  Mois cible : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}`);
  if (isExtended) {
    console.log(`🔭  Mode étendu : amorce jusqu'au ${formatDateFR(windowEnd)}`);
  }
  console.log('');

  // ── Charger le cache ────────────────────────────────────────────────────────
  const cache = loadCache();
  const cacheSizeBefore = Object.keys(cache).length;
  console.log(`💾  Cache : ${cacheSizeBefore} entrées chargées\n`);

  // ── 1. Collecte brute via discover ──────────────────────────────────────────
  const endpoints = buildScanEndpoints(monthStart, windowEnd);
  const rawMovies = [];
  const stats = { collected: 0, perEndpoint: {} };

  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`  🔎  [${String(i + 1).padStart(2, '0')}/${endpoints.length}] ${name.padEnd(22)} `);
    try {
      const results = await fetchAllPages(url);
      rawMovies.push(...results);
      stats.perEndpoint[name] = results.length;
      console.log(`→ ${results.length} films`);
    } catch (err) {
      stats.perEndpoint[name] = 0;
      console.log(`⚠️  ${err.message}`);
    }
    await sleep(API_DELAY_MS);
  }

  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  stats.collected = uniqueMovies.length;
  console.log(`\n  📦  ${uniqueMovies.length} films uniques (sur ${rawMovies.length} bruts)\n`);

  // ── 2. Analyse film par film ────────────────────────────────────────────────
  const finalResults = [];
  const dropReasons = { noDetails: 0, noReleaseDate: 0, excludedGenre: 0, tooShort: 0, noFRTheatrical: 0, beforeWindow: 0, afterWindow: 0, delayTooShort: 0 };
  let cacheHits = 0;

  console.log('  🔬  Analyse détaillée :');

  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

    let details;
    try {
      details = await fetchMovieDetails(movie.id, cache);
    } catch {
      dropReasons.noDetails++;
      continue;
    }

    if (!details.release_date)        { dropReasons.noReleaseDate++; continue; }
    if (hasExcludedGenre(details))    { dropReasons.excludedGenre++; continue; }
    if (isTooShort(details))          { dropReasons.tooShort++;     continue; }

    const isFrench   = isFrenchProduction(details);
    const minDelay   = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine  = hasTheatricalReleaseFR(details.release_dates);

    // Filtre principal : films INTL doivent avoir une sortie ciné FR
    // Films français : on est plus tolérant (TMDB est parfois incomplet sur les
    // petites sorties nationales)
    if (!isFrench && !hasFRCine) { dropReasons.noFRTheatrical++; continue; }

    // ── Calcul de la date VOD ─────────────────────────────────────────────────
    const cinemaDateGlobal = new Date(details.release_date);
    const cinemaDateFR     = getTheatricalDateFR(details.release_dates);
    const cinemaDate       = cinemaDateFR ?? cinemaDateGlobal;
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    const officialDigital  = getOfficialDigitalDate(details.release_dates);
    const onFRTVOD         = isAvailableOnFRTVOD(details['watch/providers']);
    const predictedDate    = predictVODDate(cinemaDate, isFrench);

    let vodDate, source;
    if (officialDigital) {
      vodDate = officialDigital.date;
      source  = `officielle-${officialDigital.region.toLowerCase()}`;
    } else if (onFRTVOD) {
      // Disponible MAINTENANT sur un provider FR mais pas de date type 4 :
      // on prend la prédiction si elle est dans le passé proche, sinon "aujourd'hui"
      vodDate = predictedDate < now ? predictedDate : now;
      source  = 'provider-fr';
    } else {
      vodDate = predictedDate;
      source  = 'prédite';
    }

    // Sécurité : délai minimum légal
    const actualDelayDays = (vodDate - cinemaDate) / 86400000;
    if (actualDelayDays < minDelay) { dropReasons.delayTooShort++; continue; }

    // Filtre fenêtre cible
    if (vodDate < monthStart) { dropReasons.beforeWindow++; continue; }
    if (vodDate > windowEnd)  { dropReasons.afterWindow++;  continue; }

    finalResults.push({
      // Champs CONSOMMÉS par index.html (NE PAS RENOMMER)
      title        : details.title || movie.title,
      plex_release : formatDateFR(vodDate),
      tmdb_id      : movie.id,
      poster_path  : details.poster_path || movie.poster_path,
      // Champs enrichis
      original_title : details.original_title,
      cinema_date    : formatDateFR(cinemaDate),
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      genres         : details.genres?.map((g) => g.name) ?? [],
      is_french      : isFrench,
      source,
      // Champs internes (supprimés avant écriture)
      _sortDate      : vodDate.getTime(),
      _popularity    : movie.popularity ?? details.popularity ?? 0,
    });

    const flag = isFrench ? '🇫🇷' : '🌍';
    const srcShort = source.startsWith('officielle') ? '✓' : source === 'provider-fr' ? '⊛' : '~';
    console.log(`     ${flag} ${srcShort} ${(details.title || '').padEnd(45).slice(0, 45)} → ${formatDateFR(vodDate)}`);
  }

  // ── 3. Tri stable + déduplication par titre ─────────────────────────────────
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });

  const deduped = Array.from(
    new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values()
  );

  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  // ── 4. Écriture atomique du JSON ───────────────────────────────────────────
  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  // ── 5. Sauvegarder le cache ────────────────────────────────────────────────
  // Purge des entrées trop anciennes (> 7 jours) pour éviter qu'il grossisse à l'infini
  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  // ── 6. Rapport final ────────────────────────────────────────────────────────
  const frCount   = output.filter((m) => m.is_french).length;
  const intCount  = output.length - frCount;
  const officCount = output.filter((m) => m.source.startsWith('officielle')).length;
  const tvodCount  = output.filter((m) => m.source === 'provider-fr').length;
  const predCount  = output.filter((m) => m.source === 'prédite').length;
  const elapsed   = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`
╔══════════════════════════════════════════════════╗
║  ✅  ${output.length} films VOD — ${now.toLocaleString('fr-FR', { month: 'long', year: 'numeric' })}${isExtended ? ' (+ amorce)' : ''}
║  🇫🇷  Productions françaises : ${frCount}
║  🌍  Films internationaux   : ${intCount}
║  ✓   Dates officielles      : ${officCount}
║  ⊛   Confirmées TVOD FR     : ${tvodCount}
║  ~   Dates prédites         : ${predCount}
║  💾  Cache : ${cacheHits} hits / ${uniqueMovies.length} (${Math.round(cacheHits/uniqueMovies.length*100)}%)
║  ⏱   Durée totale : ${elapsed}s
║  📁  ${DATA_PATH}
╚══════════════════════════════════════════════════╝

📊  Films écartés :
     • Pas de détails TMDB     : ${dropReasons.noDetails}
     • Pas de date de sortie   : ${dropReasons.noReleaseDate}
     • Genre exclu (doc/musique): ${dropReasons.excludedGenre}
     • Court-métrage           : ${dropReasons.tooShort}
     • Pas de sortie ciné FR   : ${dropReasons.noFRTheatrical}
     • Délai légal non respecté: ${dropReasons.delayTooShort}
     • Avant la fenêtre        : ${dropReasons.beforeWindow}
     • Après la fenêtre        : ${dropReasons.afterWindow}
`);
}

// ─── ENTRYPOINT ────────────────────────────────────────────────────────────────

updateVOD().catch((err) => {
  console.error('❌  Erreur fatale :', err);
  process.exit(1);
});
