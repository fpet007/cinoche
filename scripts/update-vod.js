/**
 * updateVOD.js
 * ============
 * Génère la liste des films disponibles en VOD pour le MOIS EN COURS.
 * - Films français (production FR) : délai légal de 4 mois (120 jours min) après sortie ciné
 * - Films américains / internationaux : délai de 45 jours après sortie ciné
 * - Priorité toujours donnée aux dates officielles TMDB si disponibles
 * - Exclut les VF/doublages : on veut les films originaux avec sortie FR
 * - Se remet à zéro automatiquement au 1er de chaque mois
 *
 * Usage : node updateVOD.js
 * Cron recommandé : 0 2 * * * (tous les soirs à 2h)
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH    = path.join(__dirname, '../data/plex-upcoming.json');

// Délais VOD en jours après sortie cinéma
const DELAYS = {
  FRENCH:        120,   // Obligation légale française : 4 mois minimum
  AMERICAN:       45,   // Standard US (PVOD / SVOD)
  DEFAULT:        45,
};

// IDs TMDB des grands studios américains (pour info, non utilisé pour les délais)
const STUDIO_IDS = {
  UNIVERSAL : [33, 12248],
  WARNER    : [174, 2734],
  DISNEY    : [2, 420, 3, 1632],
  SONY      : [5, 34],
  PARAMOUNT : [4, 60],
  NETFLIX   : [213],
  AMAZON    : [1024, 20580],
  APPLE     : [2251690],
};

// Délai entre chaque appel TMDB pour éviter le rate-limit (40 req / 10s)
const API_DELAY_MS = 120;

// ─── HELPERS ───────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Fetch avec retry automatique (3 tentatives, backoff exponentiel)
 */
async function fetchWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (attempt === retries) throw err;
      const wait = attempt * 500;
      console.warn(`  ⚠️  Tentative ${attempt} échouée (${err.message}), retry dans ${wait}ms...`);
      await sleep(wait);
    }
  }
}

/**
 * Retourne true si le film est une production française
 * (pays de production FR OU langue originale française)
 */
function isFrenchProduction(details) {
  const hasFRCountry = details.production_countries?.some(
    (c) => c.iso_3166_1 === 'FR'
  );
  const isFrLang = details.original_language === 'fr';
  return hasFRCountry || isFrLang;
}

/**
 * Cherche une date de sortie officielle digitale (type 4 = Digital)
 * en priorité sur FR, puis US.
 */
function getOfficialDigitalDate(releaseDates) {
  if (!releaseDates?.results) return null;

  for (const region of ['FR', 'US']) {
    const entry = releaseDates.results.find((r) => r.iso_3166_1 === region);
    if (!entry) continue;

    const digital = entry.release_dates.find((rd) => rd.type === 4);
    if (digital?.release_date) {
      const d = new Date(digital.release_date);
      if (!isNaN(d)) return d;
    }
  }
  return null;
}

/**
 * Calcule la date VOD prédictive à partir de la date ciné
 */
function predictVODDate(cinemDate, isFrench) {
  const delay = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
  const vod   = new Date(cinemDate);
  vod.setDate(vod.getDate() + delay);
  return vod;
}

/**
 * Formate une date en français lisible
 */
function formatDateFR(date) {
  return date.toLocaleDateString('fr-FR', {
    day  : 'numeric',
    month: 'long',
    year : 'numeric',
  });
}

// ─── RÉCUPÉRATION DES FILMS ────────────────────────────────────────────────────

/**
 * Récupère toutes les pages d'un endpoint TMDB discover (jusqu'à 5 pages max)
 */
async function fetchAllPages(baseUrl, maxPages = 5) {
  const movies = [];
  for (let page = 1; page <= maxPages; page++) {
    const url  = `${baseUrl}&page=${page}`;
    const data = await fetchWithRetry(url);
    if (!data.results?.length) break;
    movies.push(...data.results);
    if (page >= data.total_pages) break;
    await sleep(API_DELAY_MS);
  }
  return movies;
}

/**
 * Construit les endpoints de scan en fonction du mois cible.
 *
 * Logique :
 * - Films FR : sortie ciné entre (mois_cible - 5 mois) et (mois_cible - 4 mois)
 *   car délai légal = 4 mois → on scanne les sorties ciné qui arrivent à échéance ce mois
 * - Films US  : sortie ciné entre (mois_cible - 2 mois) et (mois_cible - 1 mois)
 *   car délai = 45 jours (~1.5 mois)
 */
function buildScanWindows(targetYear, targetMonth) {
  // targetMonth est 0-indexed (JS)
  const startOfTarget = new Date(targetYear, targetMonth, 1);

  // Fenêtre films FR : ciné sorti 5→4 mois avant le début du mois cible
  const frStart = new Date(startOfTarget); frStart.setMonth(frStart.getMonth() - 5);
  const frEnd   = new Date(startOfTarget); frEnd.setMonth(frEnd.getMonth() - 3);   // un peu de marge

  // Fenêtre films US : ciné sorti 3→1 mois avant le début du mois cible
  const usStart = new Date(startOfTarget); usStart.setMonth(usStart.getMonth() - 3);
  const usEnd   = new Date(startOfTarget); usEnd.setMonth(usEnd.getMonth() - 1);

  const fmt = (d) => d.toISOString().split('T')[0];

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&sort_by=popularity.desc&vote_count.gte=10`;

  return [
    // Films de production française
    `${base}&region=FR&with_origin_country=FR&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}`,
    // Films internationaux sortis en France
    `${base}&region=FR&primary_release_date.gte=${fmt(usStart)}&primary_release_date.lte=${fmt(usEnd)}`,
    // Filet de sécurité : films populaires récents sans filtre région
    `${base}&primary_release_date.gte=${fmt(usStart)}&primary_release_date.lte=${fmt(usEnd)}&with_release_type=3|2`,
  ];
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function updateVOD() {
  console.log('🎬 Démarrage updateVOD...');

  const today       = new Date();
  const targetMonth = today.getMonth();      // 0-indexed
  const targetYear  = today.getFullYear();

  // Bornes du mois cible (du 1er au dernier jour)
  const monthStart = new Date(targetYear, targetMonth, 1);
  const monthEnd   = new Date(targetYear, targetMonth + 1, 0, 23, 59, 59);

  console.log(`📅 Mois cible : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}`);

  // 1. Collecte des films bruts
  const endpoints = buildScanWindows(targetYear, targetMonth);
  let rawMovies   = [];

  for (const endpoint of endpoints) {
    console.log('  🔎 Scan endpoint...');
    const results = await fetchAllPages(endpoint, 5);
    rawMovies.push(...results);
    await sleep(API_DELAY_MS);
  }

  // Déduplication par ID TMDB
  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  console.log(`  📦 ${uniqueMovies.length} films uniques récupérés, analyse en cours...`);

  // 2. Analyse détaillée film par film
  const finalResults = [];

  for (const movie of uniqueMovies) {
    await sleep(API_DELAY_MS);

    let details;
    try {
      details = await fetchWithRetry(
        `https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`
      );
    } catch (err) {
      console.warn(`  ⚠️  Impossible de récupérer les détails de "${movie.title}" : ${err.message}`);
      continue;
    }

    // Vérifications de base
    if (!details.release_date) continue;

    const cinemDate = new Date(details.release_date);
    if (isNaN(cinemDate)) continue;

    const isFrench  = isFrenchProduction(details);
    const minDelay  = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;

    // Calcul de la date VOD
    const officialDate = getOfficialDigitalDate(details.release_dates);
    const predictedDate = predictVODDate(cinemDate, isFrench);
    const vodDate = officialDate ?? predictedDate;

    // Sécurité : vérifier que le délai minimum légal est respecté
    const actualDelay = (vodDate - cinemDate) / (1000 * 3600 * 24);
    if (actualDelay < minDelay) continue;

    // Filtre : la date VOD doit tomber dans le mois cible
    if (vodDate < monthStart || vodDate > monthEnd) continue;

    // Genres
    const genres = details.genres?.map((g) => g.name) ?? [];

    finalResults.push({
      title          : details.title || movie.title,
      original_title : details.original_title,
      vod_date       : formatDateFR(vodDate),
      vod_date_iso   : vodDate.toISOString().split('T')[0],
      cinema_date    : formatDateFR(cinemDate),
      tmdb_id        : movie.id,
      poster_path    : movie.poster_path,
      overview       : details.overview,
      vote_average   : details.vote_average,
      genres,
      is_french      : isFrench,
      source         : officialDate ? 'officielle' : 'prédite',
      _sort          : vodDate.getTime(),
    });

    console.log(`  ✅ ${details.title} → VOD le ${formatDateFR(vodDate)} (${isFrench ? 'FR' : 'US'}, ${officialDate ? 'date officielle' : 'prédiction'})`);
  }

  // 3. Tri par date VOD + déduplication par titre
  finalResults.sort((a, b) => a._sort - b._sort);

  const deduped = Array.from(
    new Map(finalResults.map((m) => [m.title.toLowerCase(), m])).values()
  );

  // Suppression du champ interne _sort
  const output = deduped.map(({ _sort, ...rest }) => rest);

  // 4. Écriture du fichier JSON
  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  fs.writeFileSync(DATA_PATH, JSON.stringify(output, null, 2), 'utf8');

  console.log(`\n✅ Terminé ! ${output.length} films VOD pour ${today.toLocaleString('fr-FR', { month: 'long', year: 'numeric' })}`);
  console.log(`📁 Fichier écrit : ${DATA_PATH}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
