/**
 * updateVOD.js  —  v3
 * ===================
 * Génère la liste des films VOD du MOIS EN COURS pour un serveur Plex FR.
 *
 * Règles métier :
 *  - Films français (prod. FR ou langue fr) : délai légal 120 jours (4 mois) après sortie ciné
 *  - Films US / internationaux             : délai standard 45 jours après sortie ciné
 *  - Dates officielles TMDB (type 4)       : priorité absolue sur la prédiction
 *  - Filtre qualité                        : le film DOIT avoir eu une sortie ciné en France
 *                                            (release_type 3 = theatrical) → élimine les films
 *                                            sans distribution FR, les films chinois/indiens
 *                                            sortis uniquement dans leur pays, etc.
 *  - Scan FR élargi                        : 6 endpoints dédiés aux productions françaises
 *                                            pour ne rien manquer
 *  - Remise à zéro automatique             : le 1er de chaque mois, les résultats du mois
 *                                            précédent sont écrasés
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * *   (tous les soirs à 2h du matin)
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH    = path.join(__dirname, '../data/plex-upcoming.json');

const DELAYS = {
  FRENCH  : 120,   // Obligation légale France : 4 mois minimum
  AMERICAN:  45,   // Standard PVOD/SVOD international
};

// Délai entre appels TMDB — ne pas descendre sous 100ms (limite : 40 req/10s)
const API_DELAY_MS = 130;

// ─── HELPERS ───────────────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** fetch avec retry x3 + backoff exponentiel */
async function fetchWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(attempt * 600);
    }
  }
}

/** Récupère toutes les pages d'un endpoint discover (max maxPages) */
async function fetchAllPages(baseUrl, maxPages = 6) {
  const movies = [];
  for (let page = 1; page <= maxPages; page++) {
    const data = await fetchWithRetry(`${baseUrl}&page=${page}`);
    if (!data.results?.length) break;
    movies.push(...data.results);
    if (page >= data.total_pages) break;
    await sleep(API_DELAY_MS);
  }
  return movies;
}

/** Le film est-il une production française ? */
function isFrenchProduction(details) {
  return (
    details.original_language === 'fr' ||
    details.production_countries?.some((c) => c.iso_3166_1 === 'FR')
  );
}

/**
 * Le film a-t-il eu une sortie ciné EN FRANCE (type 3 = theatrical) ?
 * C'est notre filtre principal contre les films sans distribution FR.
 * On accepte aussi type 2 (limited theatrical) pour les films d'auteur.
 */
function hasTheatricalReleaseFR(releaseDates) {
  if (!releaseDates?.results) return false;
  const fr = releaseDates.results.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return false;
  return fr.release_dates.some((rd) => rd.type === 3 || rd.type === 2);
}

/**
 * Date de sortie CINÉ en France (type 3 ou 2).
 * Si disponible, on l'utilise à la place de release_date global
 * pour un calcul VOD plus précis.
 */
function getTheatricalDateFR(releaseDates) {
  if (!releaseDates?.results) return null;
  const fr = releaseDates.results.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const theatrical = fr.release_dates
    .filter((rd) => rd.type === 3 || rd.type === 2)
    .sort((a, b) => new Date(a.release_date) - new Date(b.release_date))[0];
  if (!theatrical?.release_date) return null;
  const d = new Date(theatrical.release_date);
  return isNaN(d) ? null : d;
}

/**
 * Date de sortie digitale officielle (type 4) — priorité FR puis US.
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

/** Calcule la date VOD prédictive */
function predictVODDate(cinemDate, isFrench) {
  const vod = new Date(cinemDate);
  vod.setDate(vod.getDate() + (isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN));
  return vod;
}

/** Formate une date en "29 avril 2026" — format attendu par index.html */
function formatDateFR(date) {
  return date.toLocaleDateString('fr-FR', {
    day: 'numeric', month: 'long', year: 'numeric',
  });
}

// ─── CONSTRUCTION DES FENÊTRES DE SCAN ────────────────────────────────────────

/**
 * Construit tous les endpoints TMDB à scanner pour le mois cible.
 *
 * VOLET FR (6 endpoints) — productions françaises & francophones
 *   Délai légal = 120 jours → fenêtre ciné : (mois - 5.5 mois) à (mois - 3 mois)
 *
 * VOLET INTL (2 endpoints) — films avec sortie ciné en France
 *   Délai standard = 45 jours → fenêtre ciné : (mois - 3 mois) à (mois - 1 mois)
 *   Filtre : region=FR + with_release_type=3 → uniquement distribués en salle en France
 */
function buildScanEndpoints(targetYear, targetMonth) {
  const startOfTarget = new Date(targetYear, targetMonth, 1);
  const fmt = (d) => d.toISOString().split('T')[0];

  // Fenêtre FR
  const frStart = new Date(startOfTarget); frStart.setDate(frStart.getDate() - 167); // ~5.5 mois
  const frEnd   = new Date(startOfTarget); frEnd.setMonth(frEnd.getMonth() - 3);

  // Fenêtre internationale
  const usStart = new Date(startOfTarget); usStart.setMonth(usStart.getMonth() - 3);
  const usEnd   = new Date(startOfTarget); usEnd.setDate(usEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR`;

  return [
    // ── VOLET FR : 6 endpoints pour maximiser la couverture ──────────────────

    // 1. Prod FR (filtre pays) — popularité
    `${base}&with_origin_country=FR&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}&sort_by=popularity.desc`,

    // 2. Prod FR (filtre pays) — mieux notés (rattrape les films d'auteur)
    `${base}&with_origin_country=FR&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}&sort_by=vote_average.desc&vote_count.gte=20`,

    // 3. Langue fr (co-productions francophones : Belgique, Suisse, Québec…)
    `${base}&with_original_language=fr&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}&sort_by=popularity.desc`,

    // 4. Langue fr — mieux notés
    `${base}&with_original_language=fr&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}&sort_by=vote_average.desc&vote_count.gte=15`,

    // 5. Sortie theatrical en FR sur la fenêtre FR (filet de sécurité)
    `${base}&region=FR&with_original_language=fr&primary_release_date.gte=${fmt(frStart)}&primary_release_date.lte=${fmt(frEnd)}&with_release_type=3|2&sort_by=popularity.desc`,

    // 6. Prod FR sans limite basse stricte — rattrape les sorties tardives
    `${base}&with_origin_country=FR&primary_release_date.gte=${fmt(frStart)}&sort_by=release_date.desc`,

    // ── VOLET INTL : distribués en salle en France uniquement ────────────────

    // 7. Populaires — sortis en salle en France
    `${base}&region=FR&with_release_type=3&primary_release_date.gte=${fmt(usStart)}&primary_release_date.lte=${fmt(usEnd)}&sort_by=popularity.desc&vote_count.gte=30`,

    // 8. Bien notés — sortis en salle en France
    `${base}&region=FR&with_release_type=3&primary_release_date.gte=${fmt(usStart)}&primary_release_date.lte=${fmt(usEnd)}&sort_by=vote_average.desc&vote_count.gte=50`,
  ];
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function updateVOD() {
  console.log('🎬  updateVOD v3 — démarrage...\n');

  const today       = new Date();
  const targetMonth = today.getMonth();
  const targetYear  = today.getFullYear();

  const monthStart = new Date(targetYear, targetMonth, 1);
  const monthEnd   = new Date(targetYear, targetMonth + 1, 0, 23, 59, 59);

  console.log(`📅  Mois cible : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  // ── 1. Collecte brute ───────────────────────────────────────────────────────
  const endpoints = buildScanEndpoints(targetYear, targetMonth);
  let rawMovies   = [];

  for (let i = 0; i < endpoints.length; i++) {
    console.log(`  🔎  Endpoint ${i + 1}/${endpoints.length}...`);
    try {
      const results = await fetchAllPages(endpoints[i], 6);
      rawMovies.push(...results);
      console.log(`      → ${results.length} films récupérés`);
    } catch (err) {
      console.warn(`      ⚠️  Échec endpoint ${i + 1} : ${err.message}`);
    }
    await sleep(API_DELAY_MS);
  }

  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  console.log(`\n  📦  ${uniqueMovies.length} films uniques — analyse détaillée...\n`);

  // ── 2. Analyse film par film ────────────────────────────────────────────────
  const finalResults = [];
  let skipped = 0;

  for (const movie of uniqueMovies) {
    await sleep(API_DELAY_MS);

    let details;
    try {
      details = await fetchWithRetry(
        `https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`
      );
    } catch (err) {
      console.warn(`  ⚠️  Détails indisponibles pour tmdb:${movie.id} — ignoré`);
      skipped++;
      continue;
    }

    if (!details.release_date) { skipped++; continue; }

    const isFrench = isFrenchProduction(details);
    const minDelay = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;

    // ── FILTRE PRINCIPAL : sortie ciné en France obligatoire pour les films INTL ──
    // Les films français sont conservés même si TMDB manque de données FR,
    // car les petites sorties nationales ne sont pas toujours bien renseignées.
    const hasFRRelease = hasTheatricalReleaseFR(details.release_dates);
    if (!isFrench && !hasFRRelease) {
      skipped++;
      continue;
    }

    // ── Calcul de la date VOD ─────────────────────────────────────────────────
    // Priorité à la date de sortie ciné FR (plus précise que la date globale)
    const cinemDateGlobal = new Date(details.release_date);
    const cinemDateFR     = getTheatricalDateFR(details.release_dates);
    const cinemDate       = cinemDateFR ?? cinemDateGlobal;

    if (isNaN(cinemDate)) { skipped++; continue; }

    const officialDate  = getOfficialDigitalDate(details.release_dates);
    const predictedDate = predictVODDate(cinemDate, isFrench);
    const vodDate       = officialDate ?? predictedDate;

    // Sécurité : délai minimum légal
    const actualDelay = (vodDate - cinemDate) / 86400000;
    if (actualDelay < minDelay) { skipped++; continue; }

    // Filtre : date VOD dans le mois cible
    if (vodDate < monthStart || vodDate > monthEnd) { skipped++; continue; }

    const genres = details.genres?.map((g) => g.name) ?? [];

    finalResults.push({
      // Champs obligatoires pour index.html — NE PAS RENOMMER
      title        : details.title || movie.title,
      plex_release : formatDateFR(vodDate),
      tmdb_id      : movie.id,
      poster_path  : movie.poster_path,
      // Champs enrichis (utilisables par le front pour badges, filtres, etc.)
      original_title : details.original_title,
      cinema_date    : formatDateFR(cinemDate),
      overview       : details.overview,
      vote_average   : details.vote_average,
      genres,
      is_french      : isFrench,
      source         : officialDate ? 'officielle' : 'prédite',
      _sort          : vodDate.getTime(),
    });

    const flag = isFrench ? '🇫🇷' : '🌍';
    const src  = officialDate ? '✓ officielle' : '~ prédite';
    console.log(`  ${flag}  ${details.title}  →  ${formatDateFR(vodDate)}  [${src}]`);
  }

  // ── 3. Tri chronologique + déduplication par titre ──────────────────────────
  finalResults.sort((a, b) => a._sort - b._sort);

  const deduped = Array.from(
    new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values()
  );

  const output = deduped.map(({ _sort, ...rest }) => rest);

  // ── 4. Écriture du fichier JSON ─────────────────────────────────────────────
  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  fs.writeFileSync(DATA_PATH, JSON.stringify(output, null, 2), 'utf8');

  // ── 5. Rapport final ────────────────────────────────────────────────────────
  const frCount  = output.filter((m) => m.is_french).length;
  const intCount = output.length - frCount;
  const offic    = output.filter((m) => m.source === 'officielle').length;
  const pred     = output.filter((m) => m.source === 'prédite').length;

  console.log(`
╔══════════════════════════════════════════════════╗
║  ✅  ${output.length} films VOD — ${today.toLocaleString('fr-FR', { month: 'long', year: 'numeric' })}
║  🇫🇷  Productions françaises : ${frCount}
║  🌍  Films internationaux   : ${intCount}
║  ✓   Dates officielles      : ${offic}
║  ~   Dates prédites         : ${pred}
║  ⏭   Films ignorés/filtrés  : ${skipped}
║  📁  ${DATA_PATH}
╚══════════════════════════════════════════════════╝`);
}

updateVOD().catch((err) => {
  console.error('❌  Erreur fatale :', err);
  process.exit(1);
});
