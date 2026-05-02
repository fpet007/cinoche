/**
 * updateVOD.js — v7
 * =================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle.
 *
 * Nouveautés v7 :
 * ✅ Scraping MaxBlizz.com pour récupérer les dates VOD US officielles
 *    (source plus fiable que la prédiction TMDB, souvent annoncée 2-3 sem. avant la sortie)
 * ✅ Snap to Tuesday : ta règle empirique « les blockbusters US sortent le mardi »
 *    appliquée aux dates PRÉDITES pour films US uniquement (les officielles restent intactes).
 * ✅ Système d'overrides manuels (MANUAL_OVERRIDES) pour cas connus / corrections perso.
 * ✅ Hiérarchie des sources : override > maxblizz > officielle TMDB > prédite (snap mardi US).
 * ✅ Cache séparé pour le scraping MaxBlizz (12h TTL).
 * ✅ Matching par titre intelligent (normalisation, accents, sous-titres).
 *
 * Hérité de v6 (conservé) :
 * ✅ Mois strict (pas de lookahead).
 * ✅ Filtre anti-fantôme/anti-théâtre composite (vote_count >= 5 OU popularity >= 1.5).
 * ✅ Mots-clés enrichis anti-captation théâtre/spectacle.
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * * (tous les soirs à 2h)
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH        = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH       = path.join(__dirname, '../data/.tmdb-cache.json');
const MAXBLIZZ_CACHE   = path.join(__dirname, '../data/.maxblizz-cache.json');

const DELAYS = {
  FRENCH  : 120,   // Chronologie des médias FR — VOD à l'acte
  AMERICAN:  45,   // PVOD/TVOD international standard
};

// Cache TMDB
const CACHE_TTL_HOURS = 24;
// Cache MaxBlizz (plus court : on veut capter les nouvelles annonces)
const MAXBLIZZ_TTL_HOURS = 12;
// Délai entre appels (TMDB tolère ~50 req/s ; on reste très conservateur)
const API_DELAY_MS = 130;
// Pages max par endpoint discover
const MAX_PAGES_PER_ENDPOINT = 6;
// Durée minimum pour ne pas être un court-métrage (minutes)
const MIN_RUNTIME = 40;

// Genres TMDB exclus : Documentaire (99), Musique (10402), Téléfilm (10770)
const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]);

/**
 * Mots-clés titres qui trahissent une captation de spectacle vivant ou théâtre.
 */
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

/**
 * Mots-clés titres qui trahissent un téléfilm policier régional FR.
 */
const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

/**
 * ─── OVERRIDES MANUELS ──────────────────────────────────────────────────────
 * Source de vérité prioritaire. Format : { tmdbId|titleNorm: { date: 'YYYY-MM-DD', reason } }
 * Utilisé pour les films à forte attente où tu as une info plus fiable que TMDB/MaxBlizz.
 * Le titre normalisé sert de fallback si tu n'as pas le tmdb_id sous la main.
 */
const MANUAL_OVERRIDES = {
  // Exemples — à éditer / ajouter selon les besoins
  // 'project hail mary' : { date: '2026-05-12', reason: 'film à fort potentiel, repoussé' },
  // 698687               : { date: '2026-05-19', reason: 'TMDB ID prioritaire' },
};

// ─── HELPERS DE BASE ───────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url, retries = 3, opts = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, opts);
      if (res.status === 429) {
        await sleep(2000 * attempt);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return opts.text ? await res.text() : await res.json();
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

// ─── NORMALISATION TITRES (pour matching MaxBlizz / overrides) ────────────────

function normalizeTitle(s) {
  if (!s) return '';
  return s
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')   // retire les accents
    .replace(/['']/g, '')                                // retire apostrophes
    .replace(/[^a-z0-9]+/g, ' ')                         // ponctuation → espace
    .trim()
    .replace(/^the |^le |^la |^les |^l /, '');           // articles d'attaque
}

// ─── CACHE TMDB ────────────────────────────────────────────────────────────────

function loadCache(cachePath = CACHE_PATH) {
  try {
    const raw = fs.readFileSync(cachePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function saveCache(cache, cachePath = CACHE_PATH) {
  try {
    fs.mkdirSync(path.dirname(cachePath), { recursive: true });
    const tmp = cachePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, cachePath);
  } catch (err) {
    console.warn(`  ⚠️  Cache non sauvegardé : ${err.message}`);
  }
}

function isCacheEntryFresh(entry, ttlHours = CACHE_TTL_HOURS) {
  if (!entry?._cachedAt) return false;
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < ttlHours;
}

// ─── SCRAPING MAXBLIZZ ────────────────────────────────────────────────────────

/**
 * Récupère les annonces VOD de maxblizz.com.
 *
 * Stratégie :
 *  1. Page liste → liens d'articles + titres extraits des slugs
 *  2. Pour chaque article (avec cache par URL pour ne pas tout refaire à chaque run),
 *     on fetche le corps de l'article et on extrait la VRAIE date VOD.
 *
 * Pourquoi pas juste le alt de la liste ? Parce que MaxBlizz a des alts
 * incohérents (ex : Mario Galaxy alt="May 5, 2026" mais l'article dit "May 19, 2026").
 * La date dans le corps de l'article est canonique, en gras (markdown **...**),
 * formulée comme "...starting Month Day, Year" ou "...on Month Day, Year".
 *
 * Retourne un Map<titleNormalized, { date: Date, originalTitle: string, url: string }>
 */
async function fetchMaxblizzReleases() {
  // Version du parseur : si elle change, on invalide tout le cache.
  // À incrémenter à chaque modification de la logique d'extraction de date.
  const PARSER_VERSION = 2;

  const cache = loadCache(MAXBLIZZ_CACHE);
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const articleCache = cacheValid ? (cache.articles || {}) : {};
  const listFresh = cacheValid && cache._listCachedAt
    && (Date.now() - cache._listCachedAt) / 3600000 < MAXBLIZZ_TTL_HOURS;

  // Si la liste ET tous les articles sont frais, on ressort le map cache directement.
  if (listFresh && cache.releases) {
    console.log(`  💾 MaxBlizz : cache valide (${Object.keys(cache.releases).length} entrées)`);
    return new Map(Object.entries(cache.releases).map(([k, v]) => [k, {
      ...v, date: new Date(v.date),
    }]));
  }

  if (!cacheValid && cache._parserVersion !== undefined) {
    console.log(`  🔄 MaxBlizz : invalidation cache (parser v${cache._parserVersion} → v${PARSER_VERSION})`);
  }

  console.log(`  🌐 MaxBlizz : scraping en cours...`);
  const releases = new Map();

  try {
    // 1. Page liste : on récupère les URLs d'articles
    const listHtml = await fetchWithRetry(
      'https://maxblizz.com/dvd-and-vod-release-dates/',
      3,
      { text: true, headers: { 'User-Agent': 'Mozilla/5.0 updateVOD-bot' } }
    );

    // Extrait les URLs d'articles VOD (pattern stable : /[slug]-vod-release-date-revealed/)
    const linkRegex = /href="(https:\/\/maxblizz\.com\/([a-z0-9-]+)-vod-release-date-revealed\/?)"/gi;
    const articles = new Map(); // url -> slug
    let m;
    while ((m = linkRegex.exec(listHtml)) !== null) {
      if (!articles.has(m[1])) articles.set(m[1], m[2]);
    }

    console.log(`     ${articles.size} articles VOD trouvés sur la liste`);

    // 2. Pour chaque article : cache hit ou fetch + extraction de la vraie date
    let parsed = 0, fromCache = 0, fetched = 0, skipped = 0;
    const newArticleCache = {};

    for (const [url, slug] of articles) {
      let articleData;

      // Cache article (TTL = 7 jours, les dates VOD changent rarement une fois annoncées)
      const cached = articleCache[url];
      const cachedFresh = cached && (Date.now() - cached._cachedAt) / 3600000 < 24 * 7;

      if (cachedFresh) {
        articleData = cached;
        fromCache++;
      } else {
        try {
          await sleep(250); // soft rate limit
          const html = await fetchWithRetry(url, 2, {
            text: true, headers: { 'User-Agent': 'Mozilla/5.0 updateVOD-bot' },
          });

          // ── Extraction de la VRAIE date depuis le corps ──
          // Pattern canonique : "starting **Month Day, Year**" ou "on **Month Day, Year**"
          // Le ** vient du markdown rendu en HTML <strong> ou conservé brut selon la source.
          // On cherche d'abord le pattern fort (avec contexte "starting"/"on"/etc.),
          // puis on filtre pour exclure la date de publication de l'article (en haut).
          //
          // Les dates dans MaxBlizz se présentent généralement comme :
          //   <strong>May 19, 2026</strong>
          //   ou
          //   **May 19, 2026**
          //   ou en texte brut : "starting May 19, 2026"
          const monthPattern = '(January|February|March|April|May|June|July|August|September|October|November|December)';
          const datePart = `${monthPattern}\\s+(\\d{1,2}),?\\s+(\\d{4})`;

          // Stratégie : trouver les dates "fortes" (en <strong> ou ** **) dans le corps
          const strongDateRegex = new RegExp(
            `(?:<strong>|\\*\\*)\\s*${datePart}\\s*(?:</strong>|\\*\\*)`,
            'gi'
          );
          // Stratégie de secours : dates précédées de "starting" / "on" / "rent" / "buy"
          const contextDateRegex = new RegExp(
            `(?:starting|available\\s+(?:on|to)|rent\\s+(?:on|it)|buy\\s+(?:on|it)|arrives?\\s+on|arriving\\s+on|release\\s+date\\s+(?:is|will\\s+be))\\s+${datePart}`,
            'gi'
          );

          // Skip l'en-tête (~2000 chars) où apparaît la date de publication.
          // Si l'article est trop court (< 3000 chars), on prend tout : la date
          // de publication sera filtrée plus loin par la règle "préférer 'starting/in body'".
          const body = html.length > 3000 ? html.slice(2000) : html;

          const candidates = [];
          let dm;
          while ((dm = strongDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'strong', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          while ((dm = contextDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'context', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }

          // Filtre : on garde les dates valides et plausibles (entre il y a 2 mois et dans 2 ans)
          const now = Date.now();
          const minDate = now - 60 * 86400000;
          const maxDate = now + 730 * 86400000;
          const valid = candidates
            .filter((c) => !isNaN(c.date) && c.date.getTime() >= minDate && c.date.getTime() <= maxDate);

          // On privilégie les dates "strong" (en gras) — c'est l'indication la plus forte.
          // À défaut, les dates "context" (avec préposition explicite).
          let chosenDate = null;
          const strongs = valid.filter((c) => c.src === 'strong');
          const contexts = valid.filter((c) => c.src === 'context');
          if (strongs.length > 0) {
            // Plusieurs strong ? On prend la plus précoce (= date VOD, pas date d'un autre événement)
            chosenDate = strongs.sort((a, b) => a.date - b.date)[0].date;
          } else if (contexts.length > 0) {
            chosenDate = contexts.sort((a, b) => a.date - b.date)[0].date;
          }

          articleData = {
            _cachedAt: Date.now(),
            date: chosenDate ? chosenDate.toISOString() : null,
            slug,
          };
          fetched++;
        } catch (err) {
          articleData = { _cachedAt: Date.now(), date: null, slug, error: err.message };
        }
      }

      newArticleCache[url] = articleData;

      if (!articleData.date) { skipped++; continue; }
      const date = new Date(articleData.date);
      if (isNaN(date)) { skipped++; continue; }

      // Titre déduit du slug (les slugs sont stables et fiables, même si parfois préfixés
      // d'un nom de réalisateur — le matching gère ça via génération multi-clés)
      const title = slug.replace(/-/g, ' ');
      const baseNorm = normalizeTitle(title);
      if (!baseNorm) { skipped++; continue; }

      // Génère plusieurs clés pour maximiser le matching :
      //   "lee cronins the mummy" → ["lee cronins the mummy", "cronins the mummy", "the mummy"]
      const tokens = baseNorm.split(' ').filter(Boolean);
      const keysToStore = new Set([baseNorm]);
      if (tokens.length >= 3) keysToStore.add(tokens.slice(1).join(' '));
      if (tokens.length >= 4) keysToStore.add(tokens.slice(2).join(' '));

      for (const key of keysToStore) {
        if (!key || key.length < 3) continue;
        const existing = releases.get(key);
        if (!existing || date < existing.date) {
          releases.set(key, { date, originalTitle: title, url });
        }
      }
      parsed++;
    }

    console.log(`     ✓ ${parsed} films extraits (${fromCache} depuis cache, ${fetched} fetchés, ${skipped} ignorés)`);

    // Cache : on stocke la liste + les articles + la map releases finale
    const toCache = {
      _parserVersion: PARSER_VERSION,
      _listCachedAt: Date.now(),
      articles: newArticleCache,
      releases: Object.fromEntries(
        Array.from(releases.entries()).map(([k, v]) => [k, { ...v, date: v.date.toISOString() }])
      ),
    };
    saveCache(toCache, MAXBLIZZ_CACHE);
  } catch (err) {
    console.warn(`  ⚠️  MaxBlizz scraping a échoué : ${err.message} — on continue sans.`);
  }

  return releases;
}

/**
 * Cherche un film dans la map MaxBlizz par titre (FR ou original).
 * Stratégie en 3 niveaux :
 *  1. Match exact sur la clé normalisée (titre FR ou titre original)
 *  2. Match par inclusion stricte (titre court inclus dans titre long)
 *  3. Match par tokens : tous les mots significatifs (>2 lettres) du titre TMDB
 *     présents dans la clé MaxBlizz, OU inversement.
 *
 * Note : on teste prioritairement le titre original (anglais) car MaxBlizz est
 * un site US, ses titres sont en anglais. Les titres FR des films US sont
 * souvent traduits ("The Mummy" → "Le Réveil de la Momie") et ne matcheront pas.
 */
function findMaxblizzMatch(maxblizzMap, title, originalTitle) {
  // On privilégie le titre original (anglais) en premier — MaxBlizz est en anglais.
  const candidates = [originalTitle, title].filter(Boolean).map(normalizeTitle).filter(Boolean);

  // Niveau 1 : match exact
  for (const c of candidates) {
    if (maxblizzMap.has(c)) return maxblizzMap.get(c);
  }

  // Niveau 2 : inclusion stricte (titre court contenu dans titre long)
  for (const c of candidates) {
    if (c.length < 4) continue; // évite les faux positifs sur des mots trop courts
    for (const [k, v] of maxblizzMap.entries()) {
      if (k.length < 4) continue;
      // Inclusion mot-à-mot pour éviter "mummy" → "the mummy returns"
      const cTokens = c.split(' ');
      const kTokens = k.split(' ');
      const cInK = cTokens.every((t) => kTokens.includes(t));
      const kInC = kTokens.every((t) => cTokens.includes(t));
      if (cInK || kInC) {
        // Garde-fou : il faut au moins 1 token significatif (>3 lettres)
        const sigC = cTokens.filter((t) => t.length > 3);
        const sigK = kTokens.filter((t) => t.length > 3);
        if (sigC.length > 0 && sigK.length > 0) return v;
      }
    }
  }

  // Niveau 3 : tokens communs significatifs (>3 lettres) — règle plus permissive
  // On exige qu'au moins 2 tokens significatifs soient communs (sécurité contre faux positifs)
  for (const c of candidates) {
    const cTokens = c.split(' ').filter((t) => t.length > 3);
    if (cTokens.length === 0) continue;
    for (const [k, v] of maxblizzMap.entries()) {
      const kTokens = k.split(' ').filter((t) => t.length > 3);
      if (kTokens.length === 0) continue;
      const common = cTokens.filter((t) => kTokens.includes(t));
      // Si l'un des deux titres est court (1-2 tokens significatifs), on exige
      // que TOUS ses tokens significatifs soient présents dans l'autre.
      const minTokens = Math.min(cTokens.length, kTokens.length);
      if (minTokens === 1 && common.length >= 1) return v;
      if (minTokens >= 2 && common.length >= 2) return v;
    }
  }

  return null;
}

// ─── HELPERS MÉTIER ────────────────────────────────────────────────────────────

/**
 * Détermine si un film doit suivre la chronologie des médias FR (délai 120j).
 *
 * RÈGLE STRICTE : un film n'est "français" au sens chronologique QUE s'il est
 * en langue française. Un blockbuster US co-produit avec un studio FR
 * (ex: Illumination Paris pour Mario Galaxy, Despicable Me…) reste un film
 * américain au sens distribution / chronologie : il sort en PVOD US à 45j,
 * pas après 4 mois comme un film FR à l'acte.
 *
 * Sans cette règle, des blockbusters comme Mario Galaxy étaient classés FR
 * (production_countries=['FR','US','JP'], origin=['FR']) et ratés du mois.
 */
function isFrenchProduction(details) {
  // Critère bloquant : la langue originale doit être française.
  // Sans elle, peu importe les pays de production.
  if (details.original_language !== 'fr') return false;

  // Avec la langue française, il faut au moins UN ancrage de production FR
  // pour confirmer (sinon on capte aussi les films québécois, belges, etc.,
  // qui ne suivent PAS la chronologie FR à l'acte).
  const hasFRCountry = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin  = details.origin_country?.includes('FR') ?? false;
  return hasFRCountry || hasFROrigin;
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
  const title = details.title || '';
  return TELEFILM_TITLE_PATTERNS.some((rx) => rx.test(title));
}

function isGhostEntry(details) {
  const noGenres   = !details.genres || details.genres.length === 0;
  const noVotes    = (details.vote_count ?? 0) === 0 && (details.vote_average ?? 0) === 0;
  const noPoster   = !details.poster_path;
  const badTitle   = /^untitled$|^sans titre$|^\s*$/i.test(details.title || '');
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

function predictVODDate(cinemaDate, isFrench) {
  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + (isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN));
  return vod;
}

/**
 * Snap to Tuesday (règle empirique de l'utilisateur) : pour les films US uniquement,
 * on aligne la date prédite sur le PROCHAIN mardi (jamais en arrière).
 * Les blockbusters américains sortent en VOD le mardi (PVOD/TVOD US).
 * On NE TOUCHE PAS aux dates officielles ni MaxBlizz : ces sources sont déjà précises.
 *
 * Important : on n'arrondit JAMAIS en arrière, sinon on passerait sous le délai
 * minimum chronologie médias (45j US) et le film serait éjecté du résultat.
 * Au pire on ajoute 0..6 jours, jamais on n'en retire.
 */
function snapToTuesday(date) {
  const d = new Date(date);
  const day = d.getDay(); // 0=dim, 1=lun, 2=mar, 3=mer, 4=jeu, 5=ven, 6=sam
  // Décalage vers le prochain mardi (mardi=2). Si on est déjà mardi, on reste.
  // dim=0 → +2, lun=1 → +1, mar=2 → 0, mer=3 → +6, jeu=4 → +5, ven=5 → +4, sam=6 → +3
  const delta = (2 - day + 7) % 7;
  d.setDate(d.getDate() + delta);
  return d;
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
 * v6: Mois strictement en cours, aucune anticipation sur le mois suivant.
 */
function computeTargetWindow(now = new Date()) {
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
  return { monthStart, monthEnd, windowEnd: monthEnd };
}

function buildScanEndpoints(monthStart, windowEnd) {
  const frStart = new Date(windowEnd);
  frStart.setMonth(frStart.getMonth() - 6); frStart.setDate(frStart.getDate() - 15);
  const frEnd   = new Date(monthStart); frEnd.setDate(frEnd.getDate() - 100);

  // Fenêtre INTL : on doit capter les films dont la date ciné FR est suffisamment
  // récente pour que la VOD tombe dans le mois cible (délai PVOD US ~30-45j).
  // Borne haute : monthStart + 15 jours (un film sorti à J+15 du mois cible peut
  // sortir en VOD à J+45 = J+30 du mois cible, encore dedans).
  // Borne basse : 4 mois en arrière (couvre les films à fenêtre PVOD plus longue).
  const intlStart = new Date(windowEnd);
  intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd = new Date(monthStart);
  intlEnd.setDate(intlEnd.getDate() + 15);

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

// ─── DÉTAILS FILM (avec cache) ────────────────────────────────────────────────

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

// ─── RÉSOLUTION DE LA DATE VOD ────────────────────────────────────────────────

/**
 * Détermine la date VOD finale d'un film selon la hiérarchie :
 *   1. Override manuel (par tmdb_id ou titre normalisé)
 *   2. MaxBlizz (date US annoncée)
 *   3. Date numérique officielle TMDB (FR puis US)
 *   4. Prédiction (cinemaDate + délai), avec snap to Tuesday pour les films US
 */
function resolveVODDate({ details, movie, cinemaDate, isFrench, maxblizzMap, windowEnd }) {
  // 1. Override manuel
  const titleNorm = normalizeTitle(details.title || movie.title);
  const origNorm  = normalizeTitle(details.original_title || '');
  const overrideById   = MANUAL_OVERRIDES[movie.id];
  const overrideByName = MANUAL_OVERRIDES[titleNorm] || MANUAL_OVERRIDES[origNorm];
  const override = overrideById || overrideByName;
  if (override) {
    const d = new Date(override.date);
    if (!isNaN(d)) return { date: d, source: 'override-manuel' };
  }

  // 2. MaxBlizz (films US uniquement — leur source couvre essentiellement le marché US)
  if (!isFrench) {
    const mb = findMaxblizzMatch(maxblizzMap, details.title, details.original_title);
    if (mb) {
      return { date: mb.date, source: 'maxblizz' };
    }
  }

  // 3. Date numérique officielle TMDB
  const officialDigital = getOfficialDigitalDate(details.release_dates);
  if (officialDigital) {
    return { date: officialDigital.date, source: `officielle-${officialDigital.region.toLowerCase()}` };
  }

  // 4. Prédiction
  const predicted = predictVODDate(cinemaDate, isFrench);
  // Snap to Tuesday : règle empirique pour les films US uniquement.
  // Garde-fou : si le snap pousse la date au-delà de la fin du mois cible,
  // on conserve la date prédite originale plutôt que d'éjecter le film.
  if (!isFrench) {
    const snapped = snapToTuesday(predicted);
    if (windowEnd && snapped > windowEnd && predicted <= windowEnd) {
      return { date: predicted, source: 'prédite' };
    }
    return { date: snapped, source: 'prédite-mardi' };
  }
  return { date: predicted, source: 'prédite' };
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v7 — démarrage...\n');
  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);

  console.log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  // 1. Préchargement : MaxBlizz
  console.log('  📡  Préchargement MaxBlizz :');
  const maxblizzMap = await fetchMaxblizzReleases();
  console.log('');

  // 2. Cache TMDB
  const cache = loadCache();
  console.log(`  💾  Cache TMDB : ${Object.keys(cache).length} entrées chargées\n`);

  // 3. Scan endpoints discover
  const endpoints = buildScanEndpoints(monthStart, windowEnd);
  const rawMovies = [];
  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`  🔎  [${String(i + 1).padStart(2, '0')}/${endpoints.length}] ${name.padEnd(22)} `);
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

  // 4. Analyse détaillée
  const finalResults = [];
  const dropReasons = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0,
    spectacle: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, beforeWindow: 0, afterWindow: 0, delayTooShort: 0,
  };
  let cacheHits = 0;
  const sourceStats = { 'override-manuel': 0, 'maxblizz': 0, 'officielle-fr': 0, 'officielle-us': 0, 'prédite': 0, 'prédite-mardi': 0 };

  console.log('  🔬  Analyse détaillée :');
  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

    let details;
    try {
      details = await fetchMovieDetails(movie.id, cache);
    } catch {
      dropReasons.noDetails++; continue;
    }

    if (!details.release_date)        { dropReasons.noReleaseDate++; continue; }
    if (hasExcludedGenre(details))    { dropReasons.excludedGenre++; continue; }
    if (isTelefilmByTitle(details))   { dropReasons.telefilmByTitle++; continue; }
    if (isSpectacle(details))         { dropReasons.spectacle++; continue; }
    if (isTooShort(details))          { dropReasons.tooShort++; continue; }
    if (isGhostEntry(details))        { dropReasons.ghostEntry++; continue; }

    if (!details.genres || details.genres.length === 0) {
      dropReasons.noGenresAtAll++; continue;
    }

    const isFrench   = isFrenchProduction(details);
    const minDelay   = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine  = hasTheatricalReleaseFR(details.release_dates);

    if (!hasFRCine) { dropReasons.noFRTheatrical++; continue; }

    // Filtre Qualité Composite (v6)
    const voteCount   = details.vote_count ?? 0;
    const popularity  = movie.popularity ?? details.popularity ?? 0;
    const isSafeVolume   = voteCount >= 5;
    const isNicheButReal = popularity >= 1.5;

    if (!isSafeVolume && !isNicheButReal) {
      dropReasons.lowQuality++;
      continue;
    }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    // ── Hiérarchie des sources : override > maxblizz > officielle TMDB > prédite ──
    const { date: vodDate, source } = resolveVODDate({
      details, movie, cinemaDate, isFrench, maxblizzMap, windowEnd,
    });

    // Sécurité délai : on rejette une date VOD trop proche de la sortie ciné
    // UNIQUEMENT pour les dates prédites (où l'on calcule soi-même).
    // Les sources fiables (override, maxblizz, officielles TMDB) sont acceptées telles quelles :
    // si un studio annonce une fenêtre PVOD plus courte, c'est la réalité, pas un bug.
    const isPredicted = source === 'prédite' || source === 'prédite-mardi';
    if (isPredicted) {
      const actualDelayDays = (vodDate - cinemaDate) / 86400000;
      if (actualDelayDays < minDelay) { dropReasons.delayTooShort++; continue; }
    }

    if (vodDate < monthStart) { dropReasons.beforeWindow++; continue; }
    if (vodDate > windowEnd)  { dropReasons.afterWindow++;  continue; }

    sourceStats[source] = (sourceStats[source] || 0) + 1;

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
      _sortDate      : vodDate.getTime(),
      _popularity    : popularity,
    });

    const flag = isFrench ? '🇫🇷' : '🌍';
    const srcShort = {
      'override-manuel': '★',
      'maxblizz'       : '◆',
      'officielle-fr'  : '✓',
      'officielle-us'  : '✓',
      'prédite'        : '~',
      'prédite-mardi'  : '~',
    }[source] || '?';
    console.log(`     ${flag} ${srcShort} ${(details.title || '').padEnd(45).slice(0, 45)} → ${formatDateFR(vodDate)}`);
  }

  // Tri par date VOD puis popularité
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });

  // Dédoublonnage par titre
  const deduped = Array.from(new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values());
  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  // Écriture du fichier
  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  // Nettoyage cache TMDB > 7j
  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  // ─── Stats finales ────────────────────────────────────────────────────────
  const frCount  = output.filter((m) => m.is_french).length;
  const elapsed  = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`\n  📊  Sources :`);
  for (const [src, n] of Object.entries(sourceStats)) {
    if (n > 0) console.log(`       ${src.padEnd(20)} : ${n}`);
  }
  console.log(`\n  ⏱   Durée : ${elapsed}s — Cache hits : ${cacheHits}/${uniqueMovies.length}`);
  console.log(`  ✅  Terminé : ${output.length} films générés (${frCount} 🇫🇷). Faible qualité écartés : ${dropReasons.lowQuality}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
