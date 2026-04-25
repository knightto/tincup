const crypto = require('crypto');

const DAY_OPTIONS = ['Day 1', 'Day 2A', 'Day 2B', 'Day 2 Total', 'Day 3', 'Day 4'];
const MATCH_DAY_OPTIONS = ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4', 'Practice'];
const TIN_CUP_RANK_POINTS = [12, 10, 8.5, 7, 5.75, 4.5, 3, 1.25, 0, 0, 0, 0, 0, 0, 0, 0];
const PRACTICE_DAY_KEY = 'Practice';

const PLAYERS = [
  { name: 'Matt', handicap: 10.8 }, { name: 'Rick', handicap: 15.3 }, { name: 'OB', handicap: 11.2 }, { name: 'Kyle', handicap: 8.2 },
  { name: 'Manny', handicap: 22.0 }, { name: 'Steve', handicap: 13.1 }, { name: 'Tommy', handicap: 9.1 }, { name: 'Pat', handicap: 17.6 },
  { name: 'Mil', handicap: 12.9 }, { name: 'Paul O', handicap: 15.0 }, { name: 'Brian', handicap: 20.9 }, { name: 'Bob', handicap: 22.0 },
  { name: 'David', handicap: 12.9 }, { name: 'John', handicap: 11.2 }, { name: 'Tony', handicap: 12.8 }, { name: 'Spiro', handicap: 24.5 },
];
const PLAYER_KEYS = new Set(PLAYERS.map((player) => String(player.name || '').trim().replace(/\s+/g, ' ').toLowerCase()));

const FOURSOMES = [
  { playersByDay: { 'Day 1': [{ name: 'OB', hcp: 7 }, { name: 'Rick', hcp: 8 }, { name: 'Matt', hcp: 6 }, { name: 'Tommy', hcp: 5 }], 'Day 2A': [{ name: 'Paul O', hcp: 7 }, { name: 'Tommy', hcp: 5 }, { name: 'Mil', hcp: 8 }, { name: 'Spiro', hcp: 14 }], 'Day 2B': [{ name: 'Tommy', hcp: 5 }, { name: 'Kyle', hcp: 4 }, { name: 'Steve', hcp: 8 }, { name: 'Bob', hcp: 13 }], 'Day 3': [{ name: 'Tommy', hcp: 5 }, { name: 'David', hcp: 8 }, { name: 'Tony', hcp: 8 }, { name: 'Brian', hcp: 12 }], 'Day 4': [{ name: 'OB', hcp: 7 }, { name: 'Rick', hcp: 8 }, { name: 'Matt', hcp: 6 }, { name: 'Tommy', hcp: 5 }], Practice: [{ name: 'OB', hcp: 7 }, { name: 'Rick', hcp: 8 }, { name: 'Matt', hcp: 6 }, { name: 'Tommy', hcp: 5 }], Scramble: [{ name: 'Paul O', hcp: 7 }, { name: 'David', hcp: 8 }, { name: 'Matt', hcp: 6 }, { name: 'Bob', hcp: 13 }] } },
  { playersByDay: { 'Day 1': [{ name: 'John', hcp: 7 }, { name: 'David', hcp: 8 }, { name: 'Kyle', hcp: 4 }, { name: 'Mil', hcp: 8 }], 'Day 2A': [{ name: 'Steve', hcp: 8 }, { name: 'David', hcp: 8 }, { name: 'Rick', hcp: 8 }, { name: 'Manny', hcp: 13 }], 'Day 2B': [{ name: 'OB', hcp: 7 }, { name: 'David', hcp: 8 }, { name: 'Pat', hcp: 10 }, { name: 'Spiro', hcp: 14 }], 'Day 3': [{ name: 'OB', hcp: 7 }, { name: 'Paul O', hcp: 7 }, { name: 'Kyle', hcp: 4 }, { name: 'Manny', hcp: 13 }], 'Day 4': [{ name: 'John', hcp: 7 }, { name: 'David', hcp: 8 }, { name: 'Kyle', hcp: 4 }, { name: 'Mil', hcp: 8 }], Practice: [{ name: 'John', hcp: 7 }, { name: 'David', hcp: 8 }, { name: 'Kyle', hcp: 4 }, { name: 'Mil', hcp: 8 }], Scramble: [{ name: 'Rick', hcp: 8 }, { name: 'Kyle', hcp: 4 }, { name: 'Tony', hcp: 8 }, { name: 'Spiro', hcp: 14 }] } },
  { playersByDay: { 'Day 1': [{ name: 'Tony', hcp: 8 }, { name: 'Steve', hcp: 8 }, { name: 'Pat', hcp: 10 }, { name: 'Paul O', hcp: 7 }], 'Day 2A': [{ name: 'Matt', hcp: 6 }, { name: 'Pat', hcp: 10 }, { name: 'Brian', hcp: 12 }, { name: 'Kyle', hcp: 4 }], 'Day 2B': [{ name: 'Matt', hcp: 6 }, { name: 'Manny', hcp: 13 }, { name: 'Tony', hcp: 8 }, { name: 'Mil', hcp: 8 }], 'Day 3': [{ name: 'John', hcp: 7 }, { name: 'Steve', hcp: 8 }, { name: 'Spiro', hcp: 14 }, { name: 'Matt', hcp: 6 }], 'Day 4': [{ name: 'Tony', hcp: 8 }, { name: 'Steve', hcp: 8 }, { name: 'Pat', hcp: 10 }, { name: 'Paul O', hcp: 7 }], Practice: [{ name: 'Tony', hcp: 8 }, { name: 'Steve', hcp: 8 }, { name: 'Pat', hcp: 10 }, { name: 'Paul O', hcp: 7 }], Scramble: [{ name: 'Tommy', hcp: 5 }, { name: 'John', hcp: 7 }, { name: 'Manny', hcp: 13 }, { name: 'Pat', hcp: 10 }] } },
  { playersByDay: { 'Day 1': [{ name: 'Bob', hcp: 13 }, { name: 'Manny', hcp: 13 }, { name: 'Brian', hcp: 12 }, { name: 'Spiro', hcp: 14 }], 'Day 2A': [{ name: 'Tony', hcp: 8 }, { name: 'Bob', hcp: 13 }, { name: 'OB', hcp: 7 }, { name: 'John', hcp: 7 }], 'Day 2B': [{ name: 'John', hcp: 7 }, { name: 'Rick', hcp: 8 }, { name: 'Paul O', hcp: 7 }, { name: 'Brian', hcp: 12 }], 'Day 3': [{ name: 'Mil', hcp: 8 }, { name: 'Pat', hcp: 10 }, { name: 'Bob', hcp: 13 }, { name: 'Rick', hcp: 8 }], 'Day 4': [{ name: 'Bob', hcp: 13 }, { name: 'Manny', hcp: 13 }, { name: 'Brian', hcp: 12 }, { name: 'Spiro', hcp: 14 }], Practice: [{ name: 'Bob', hcp: 13 }, { name: 'Manny', hcp: 13 }, { name: 'Brian', hcp: 12 }, { name: 'Spiro', hcp: 14 }], Scramble: [{ name: 'Mil', hcp: 8 }, { name: 'OB', hcp: 7 }, { name: 'Steve', hcp: 8 }, { name: 'Brian', hcp: 12 }] } },
];

const SEED_PLAYER_PENALTIES = {
  matt: { champion: 2, rookie: 0 },
  kyle: { champion: 1, rookie: 0 },
  tommy: { champion: 1, rookie: 0 },
  'paul o': { champion: 3, rookie: 0 },
  spiro: { champion: 0, rookie: 1 },
  bob: { champion: 0, rookie: 1 },
};
const SEED_SCRAMBLE_BONUS = {
  matt: 1,
  tommy: 1,
  spiro: 0.5,
  brian: 0.5,
};
const SEED_MARKER_HOLES = {
  ctp: [3, 7, 12, 17],
  longDrive: [5, 14],
};
const SIDE_GAME_DEFS = {
  longPutt: { label: 'Long Putt', days: ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4', 'Scramble'] },
  secretSnowman: { label: 'Secret Snowman', days: ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4'] },
};
const COMPETITIVE_DAYS = ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4'];
const ALL_HOLES = Array.from({ length: 18 }, (_, index) => index + 1);
const DAY_HOLE_STROKE_INDEX = Object.freeze({
  'Day 1': [12, 4, 18, 10, 2, 16, 14, 8, 6, 11, 17, 1, 9, 3, 13, 7, 15, 5],
  'Day 2A': [13, 3, 17, 9, 15, 7, 11, 1, 5, 2, 16, 8, 14, 4, 18, 10, 6, 12],
  'Day 2B': [5, 15, 7, 13, 9, 1, 3, 17, 11, 2, 18, 8, 10, 16, 12, 6, 14, 4],
  'Day 3': [3, 11, 17, 9, 15, 13, 7, 5, 1, 6, 10, 14, 2, 8, 12, 16, 18, 4],
  'Day 4': [11, 5, 17, 9, 7, 1, 3, 15, 13, 4, 12, 14, 16, 2, 6, 18, 8, 10],
});
const SCRAMBLE_POINTS = [3, 2, 1, 0];
const TIN_CUP_WORKBOOK_DEFAULTS = {
  handicap: {
    indexToHcConversion: 0.17920353982300874,
    avgCourseSlope: 133.25,
    avgSlope: 113,
    maxHandicap: 32,
    par3Fraction: 0.375,
  },
  accounting: {
    entryFee: 200,
    finalPayouts: [1000, 500, 250, 125],
    markerPayouts: {
      longDrive: 25,
      ctp: 25,
      longPutt: 25,
      secretSnowman: 25,
      loser: 50,
    },
    skins: {
      perPerson: 25,
    },
    scramblePoints: SCRAMBLE_POINTS.slice(),
  },
};

const clean = (v = '') => String(v || '').trim();
const normalize = (v = '') => clean(v).replace(/\s+/g, ' ').toLowerCase();
const toIntOrNull = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const out = Math.round(n);
  return out > 0 ? out : null;
};
const toNumOrNull = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const toDollar = (v) => {
  const n = toNumOrNull(v);
  return n === null ? 0 : Math.round(n);
};
const holeArray = () => Array.from({ length: 18 }, () => null);
const toPenalty = (v) => {
  const n = toNumOrNull(v);
  return n === null ? 0 : Number(n.toFixed(2));
};

function normalizePenalties(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const out = {};
  PLAYERS.forEach((player) => {
    const key = normalize(player.name);
    const raw = src[key] || src[player.name] || {};
    const champion = toPenalty(raw && raw.champion);
    const rookie = toPenalty(raw && raw.rookie);
    if (champion || rookie) {
      out[key] = { champion, rookie };
    }
  });
  return out;
}

function getPenaltyEntry(state, playerName = '') {
  const key = normalize(playerName);
  const penalties = (state && state.penalties && typeof state.penalties === 'object') ? state.penalties : {};
  const raw = penalties[key] || {};
  const champion = toPenalty(raw.champion);
  const rookie = toPenalty(raw.rookie);
  return { champion, rookie, total: Number((champion + rookie).toFixed(2)) };
}

function buildPenaltyTable(state) {
  return PLAYERS.reduce((acc, player) => {
    acc[normalize(player.name)] = getPenaltyEntry(state, player.name);
    return acc;
  }, {});
}

function defaultWorkbookConfig() {
  return JSON.parse(JSON.stringify(TIN_CUP_WORKBOOK_DEFAULTS));
}

function normalizeWorkbookConfig(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const defaults = defaultWorkbookConfig();
  const handicap = src.handicap && typeof src.handicap === 'object' ? src.handicap : {};
  const accounting = src.accounting && typeof src.accounting === 'object' ? src.accounting : {};
  const markerPayouts = accounting.markerPayouts && typeof accounting.markerPayouts === 'object' ? accounting.markerPayouts : {};
  const skins = accounting.skins && typeof accounting.skins === 'object' ? accounting.skins : {};
  const scramblePoints = Array.isArray(accounting.scramblePoints) ? accounting.scramblePoints : defaults.accounting.scramblePoints;
  return {
    handicap: {
      indexToHcConversion: toNumOrNull(handicap.indexToHcConversion) ?? defaults.handicap.indexToHcConversion,
      avgCourseSlope: toNumOrNull(handicap.avgCourseSlope) ?? defaults.handicap.avgCourseSlope,
      avgSlope: toNumOrNull(handicap.avgSlope) ?? defaults.handicap.avgSlope,
      maxHandicap: toNumOrNull(handicap.maxHandicap) ?? defaults.handicap.maxHandicap,
      par3Fraction: toNumOrNull(handicap.par3Fraction) ?? defaults.handicap.par3Fraction,
    },
    accounting: {
      entryFee: toNumOrNull(accounting.entryFee) ?? defaults.accounting.entryFee,
      finalPayouts: defaults.accounting.finalPayouts.map((value, index) => toNumOrNull((accounting.finalPayouts || [])[index]) ?? value),
      markerPayouts: {
        longDrive: toNumOrNull(markerPayouts.longDrive) ?? defaults.accounting.markerPayouts.longDrive,
        ctp: toNumOrNull(markerPayouts.ctp) ?? defaults.accounting.markerPayouts.ctp,
        longPutt: toNumOrNull(markerPayouts.longPutt) ?? defaults.accounting.markerPayouts.longPutt,
        secretSnowman: toNumOrNull(markerPayouts.secretSnowman) ?? defaults.accounting.markerPayouts.secretSnowman,
        loser: toNumOrNull(markerPayouts.loser) ?? defaults.accounting.markerPayouts.loser,
      },
      skins: {
        perPerson: toNumOrNull(skins.perPerson) ?? defaults.accounting.skins.perPerson,
      },
      scramblePoints: defaults.accounting.scramblePoints.map((value, index) => toNumOrNull(scramblePoints[index]) ?? value),
    },
  };
}


function updateWorkbookConfig(state, payload = {}) {
  const current = normalizeWorkbookConfig(state && state.config ? state.config : defaultWorkbookConfig());
  const incoming = (payload && typeof payload === 'object') ? payload : {};
  const next = {
    handicap: {
      ...current.handicap,
      ...((incoming.handicap && typeof incoming.handicap === 'object') ? incoming.handicap : {}),
    },
    accounting: {
      ...current.accounting,
      ...((incoming.accounting && typeof incoming.accounting === 'object') ? incoming.accounting : {}),
      markerPayouts: {
        ...current.accounting.markerPayouts,
        ...((incoming.accounting && incoming.accounting.markerPayouts && typeof incoming.accounting.markerPayouts === 'object') ? incoming.accounting.markerPayouts : {}),
      },
      skins: {
        ...current.accounting.skins,
        ...((incoming.accounting && incoming.accounting.skins && typeof incoming.accounting.skins === 'object') ? incoming.accounting.skins : {}),
      },
      finalPayouts: Array.isArray(incoming.accounting && incoming.accounting.finalPayouts)
        ? incoming.accounting.finalPayouts
        : current.accounting.finalPayouts,
      scramblePoints: Array.isArray(incoming.accounting && incoming.accounting.scramblePoints)
        ? incoming.accounting.scramblePoints
        : current.accounting.scramblePoints,
    },
  };
  state.config = normalizeWorkbookConfig(next);
  return state.config;
}

function holeScoreArray(input) {
  const out = Array.isArray(input) ? input.slice(0, 18) : [];
  while (out.length < 18) out.push(null);
  return out.map((value) => toIntOrNull(value));
}

function normalizeScrambleState(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const scores = src.scores && typeof src.scores === 'object' ? src.scores : {};
  const normalized = {};
  Object.keys(scores).forEach((key) => {
    normalized[key] = holeScoreArray(scores[key]);
  });
  return { scores: normalized };
}

function normalizeSideGamesState(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const out = {};
  Object.entries(SIDE_GAME_DEFS).forEach(([type, def]) => {
    const rawDays = src[type] && typeof src[type] === 'object' ? src[type] : {};
    out[type] = {};
    def.days.forEach((dayKey) => {
      const winner = String(rawDays[dayKey] || '').trim();
      const key = normalize(winner);
      out[type][dayKey] = PLAYER_KEYS.has(key)
        ? (PLAYERS.find((player) => normalize(player.name) === key) || {}).name || ''
        : '';
    });
  });
  return out;
}

function defaultTinCupLiveState() {
  return {
    version: 1,
    settings: {
      enableLiveFoursomeScoring: true,
      enableFoursomeCodes: true,
      enableLiveMarkers: true,
      enableLiveLeaderboard: true,
    },
    codes: {},
    scorecards: {},
    scrambleBonus: {},
    scramble: normalizeScrambleState(),
    sideGames: normalizeSideGamesState(),
    penalties: {},
    config: defaultWorkbookConfig(),
  };
}

function normalizeSettings(input = {}, fallback = {}) {
  return {
    enableLiveFoursomeScoring: input.enableLiveFoursomeScoring === undefined
      ? Boolean(fallback.enableLiveFoursomeScoring)
      : input.enableLiveFoursomeScoring === true,
    enableFoursomeCodes: input.enableFoursomeCodes === undefined
      ? Boolean(fallback.enableFoursomeCodes)
      : input.enableFoursomeCodes === true,
    enableLiveMarkers: input.enableLiveMarkers === undefined
      ? Boolean(fallback.enableLiveMarkers)
      : input.enableLiveMarkers === true,
    enableLiveLeaderboard: input.enableLiveLeaderboard === undefined
      ? Boolean(fallback.enableLiveLeaderboard)
      : input.enableLiveLeaderboard === true,
  };
}

function ensureTinCupLiveState(trip = {}) {
  const defaults = defaultTinCupLiveState();
  const src = (trip && trip.tinCupLive) || {};
  const out = {
    version: 1,
    settings: normalizeSettings(
      (src && typeof src.settings === 'object' && src.settings) ? src.settings : {},
      defaults.settings
    ),
    codes: (src && typeof src.codes === 'object' && src.codes) ? src.codes : {},
    scorecards: (src && typeof src.scorecards === 'object' && src.scorecards) ? src.scorecards : {},
    scrambleBonus: (src && typeof src.scrambleBonus === 'object' && src.scrambleBonus) ? src.scrambleBonus : {},
    scramble: normalizeScrambleState((src && typeof src.scramble === 'object' && src.scramble) ? src.scramble : defaults.scramble),
    sideGames: normalizeSideGamesState((src && typeof src.sideGames === 'object' && src.sideGames) ? src.sideGames : defaults.sideGames),
    penalties: normalizePenalties((src && typeof src.penalties === 'object' && src.penalties) ? src.penalties : {}),
    config: normalizeWorkbookConfig((src && typeof src.config === 'object' && src.config) ? src.config : defaults.config),
  };
  trip.tinCupLive = out;
  return out;
}

function updateSettings(state, nextSettings = {}) {
  state.settings = normalizeSettings(nextSettings, state.settings || defaultTinCupLiveState().settings);
  return state.settings;
}

function clearCompetitionState(state, options = {}) {
  const defaults = defaultTinCupLiveState();
  const preserveCodes = options.preserveCodes !== false;
  const preservePenalties = options.preservePenalties !== false;
  const preserveConfig = options.preserveConfig !== false;
  const preserveSettings = options.preserveSettings !== false;
  state.version = defaults.version;
  state.settings = preserveSettings
    ? normalizeSettings(state && state.settings ? state.settings : {}, defaults.settings)
    : defaults.settings;
  state.codes = preserveCodes
    ? JSON.parse(JSON.stringify((state && state.codes && typeof state.codes === 'object') ? state.codes : {}))
    : {};
  state.scorecards = {};
  state.scrambleBonus = {};
  state.scramble = normalizeScrambleState();
  state.sideGames = normalizeSideGamesState();
  state.penalties = preservePenalties
    ? normalizePenalties((state && state.penalties && typeof state.penalties === 'object') ? state.penalties : {})
    : {};
  state.config = preserveConfig
    ? normalizeWorkbookConfig((state && state.config && typeof state.config === 'object') ? state.config : defaults.config)
    : defaultWorkbookConfig();
  return state;
}

function getSideGameWinner(state, type = '', dayKey = '') {
  const sideGames = normalizeSideGamesState(state && state.sideGames);
  return String((((sideGames[type] || {})[dayKey]) || '')).trim();
}

function buildSideGameSummary(state) {
  const sideGames = normalizeSideGamesState(state && state.sideGames);
  return Object.entries(SIDE_GAME_DEFS).reduce((acc, [type, def]) => {
    const counts = new Map(PLAYERS.map((player) => [normalize(player.name), 0]));
    const days = def.days.map((dayKey) => {
      const winner = String((((sideGames[type] || {})[dayKey]) || '')).trim();
      const key = normalize(winner);
      if (key && counts.has(key)) counts.set(key, (counts.get(key) || 0) + 1);
      return { dayKey, winner };
    });
    acc[type] = {
      label: def.label,
      days,
      totals: PLAYERS.map((player) => ({
        name: player.name,
        wins: counts.get(normalize(player.name)) || 0,
      })).filter((row) => row.wins > 0),
    };
    return acc;
  }, {});
}

function updateSideGameWinner(state, payload = {}) {
  const type = String(payload.type || '').trim();
  const dayKey = String(payload.dayKey || '').trim();
  if (!SIDE_GAME_DEFS[type]) throw new Error('Unsupported side game type');
  if (!SIDE_GAME_DEFS[type].days.includes(dayKey)) throw new Error('Unsupported side game day');
  if (!state.sideGames || typeof state.sideGames !== 'object') state.sideGames = normalizeSideGamesState();
  state.sideGames = normalizeSideGamesState(state.sideGames);
  const winner = String(payload.winner || '').trim();
  const key = normalize(winner);
  state.sideGames[type][dayKey] = winner && PLAYER_KEYS.has(key)
    ? (PLAYERS.find((player) => normalize(player.name) === key) || {}).name || ''
    : '';
  return buildSideGameSummary(state);
}

function getSecretSnowmanCandidates(state, dayKey = '') {
  const cleanDayKey = clean(dayKey);
  if (!SIDE_GAME_DEFS.secretSnowman.days.includes(cleanDayKey)) throw new Error('Unsupported side game day');
  const incompleteSlots = [];
  const eligibleCards = [];
  getDaySlots(cleanDayKey).forEach((slot) => {
    const scorecard = getStoredScorecard(state, cleanDayKey, slot.slotIndex);
    if (!scorecard) {
      incompleteSlots.push(slot.label);
      return;
    }
    const cardCandidates = [];
    let completeCard = true;
    slot.players.forEach((player) => {
      const entry = scorecard.players[normalize(player.name)] || { name: player.name, holes: holeArray() };
      const summary = summarizePlayerCard(entry, player.hcp, cleanDayKey);
      if (!summary.complete18) {
        completeCard = false;
        return;
      }
      summary.holes.forEach((gross, index) => {
        if (gross !== 8) return;
        cardCandidates.push({
          dayKey: cleanDayKey,
          slotIndex: slot.slotIndex,
          label: slot.label,
          playerName: player.name,
          hole: index + 1,
          gross,
        });
      });
    });
    if (!completeCard) {
      incompleteSlots.push(slot.label);
      return;
    }
    if (cardCandidates.length) {
      eligibleCards.push({
        dayKey: cleanDayKey,
        slotIndex: slot.slotIndex,
        label: slot.label,
        candidates: cardCandidates,
      });
    }
  });
  if (incompleteSlots.length) {
    const error = new Error(`All scores for ${cleanDayKey} must be entered before drawing Secret Snowman. Missing: ${incompleteSlots.join(', ')}`);
    error.status = 400;
    throw error;
  }
  if (!eligibleCards.length) {
    const error = new Error(`No score of 8 was entered on ${cleanDayKey}, so Secret Snowman cannot be drawn.`);
    error.status = 400;
    throw error;
  }
  return eligibleCards;
}

function pickSecretSnowmanWinner(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const eligibleCards = getSecretSnowmanCandidates(state, dayKey);
  const pickedCard = eligibleCards[crypto.randomInt(eligibleCards.length)];
  const picked = pickedCard.candidates[crypto.randomInt(pickedCard.candidates.length)];
  return {
    picked,
    sideGames: updateSideGameWinner(state, {
      type: 'secretSnowman',
      dayKey,
      winner: picked.playerName,
    }),
  };
}

function isScorecardComplete(state, dayKey = '', slotIndex = 0) {
  const cleanDayKey = clean(dayKey);
  const slot = getDaySlots(cleanDayKey).find((item) => item.slotIndex === Number(slotIndex));
  if (!slot) return false;
  const scorecard = getStoredScorecard(state, cleanDayKey, slotIndex);
  if (!scorecard) return false;
  return slot.players.every((player) => {
    const entry = scorecard.players[normalize(player.name)] || { name: player.name, holes: holeArray() };
    const summary = summarizePlayerCard(entry, player.hcp, cleanDayKey);
    return summary.complete18 === true;
  });
}

function maybeAutoSubmitScorecard(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const slotIndex = Number(payload.slotIndex);
  if (!dayKey) throw new Error('dayKey required');
  if (!Number.isInteger(slotIndex) || slotIndex < 0) throw new Error('slotIndex required');
  const scorecard = getStoredScorecard(state, dayKey, slotIndex);
  if (!scorecard || scorecard.submittedAt) return { autoSubmitted: false, view: getScorecardView(state, dayKey, slotIndex) };
  if (!isScorecardComplete(state, dayKey, slotIndex)) return { autoSubmitted: false, view: getScorecardView(state, dayKey, slotIndex) };
  return {
    autoSubmitted: true,
    view: submitScorecard(state, {
      dayKey,
      slotIndex,
      scorerName: clean(payload.scorerName) || scorecard.scorerName || '',
    }),
  };
}

function maybeAutoDrawSecretSnowman(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  if (!SIDE_GAME_DEFS.secretSnowman.days.includes(dayKey)) return { autoDrawn: false, picked: null, sideGames: buildSideGameSummary(state) };
  if (getSideGameWinner(state, 'secretSnowman', dayKey)) {
    return { autoDrawn: false, picked: null, sideGames: buildSideGameSummary(state) };
  }
  try {
    const result = pickSecretSnowmanWinner(state, { dayKey });
    return {
      autoDrawn: true,
      picked: result.picked,
      sideGames: result.sideGames,
    };
  } catch (error) {
    const message = String(error && error.message || '');
    if (/All scores .* must be entered/i.test(message) || /No score of 8 was entered/i.test(message)) {
      return { autoDrawn: false, picked: null, sideGames: buildSideGameSummary(state) };
    }
    throw error;
  }
}

function getDaySlots(dayKey = '') {
  const cleanDayKey = clean(dayKey);
  if (cleanDayKey === PRACTICE_DAY_KEY) return [];
  return FOURSOMES.map((group, index) => {
    const dayPlayers = (group.playersByDay && group.playersByDay[cleanDayKey]) || [];
    return {
      slotIndex: index,
      label: `Group ${index + 1}`,
      players: dayPlayers.map((p) => ({ name: p.name, hcp: Number(p.hcp) || 0 })),
    };
  }).filter((slot) => slot.players.length === 4);
}

function keyFor(dayKey, slotIndex) {
  return `${clean(dayKey)}|${Number(slotIndex)}`;
}

function makeSalt() {
  return crypto.randomBytes(8).toString('hex');
}

function hashCode(salt, code) {
  const payload = `${salt}|${clean(code).toUpperCase()}`;
  return crypto.createHash('sha256').update(payload).digest('hex');
}

function generateCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let out = '';
  for (let i = 0; i < 6; i += 1) out += alphabet[Math.floor(Math.random() * alphabet.length)];
  return out;
}

function ensureSlotScorecard(state, dayKey, slotIndex) {
  const slot = getDaySlots(dayKey).find((item) => item.slotIndex === Number(slotIndex));
  if (!slot) throw new Error('Tee time slot not found for this day.');
  const k = keyFor(dayKey, slotIndex);
  if (!state.scorecards[k]) {
    state.scorecards[k] = {
      dayKey,
      slotIndex: Number(slotIndex),
      players: {},
      markers: { ctp: {}, longDrive: {} },
      scorerName: '',
      submittedAt: null,
      submittedBy: '',
      updatedAt: new Date().toISOString(),
    };
  }
  const sc = state.scorecards[k];
  if (!sc.players || typeof sc.players !== 'object') sc.players = {};
  if (!sc.markers || typeof sc.markers !== 'object') sc.markers = { ctp: {}, longDrive: {} };
  if (!sc.markers.ctp || typeof sc.markers.ctp !== 'object') sc.markers.ctp = {};
  if (!sc.markers.longDrive || typeof sc.markers.longDrive !== 'object') sc.markers.longDrive = {};
  if (typeof sc.scorerName !== 'string') sc.scorerName = sc.scorerName ? String(sc.scorerName) : '';
  if (typeof sc.submittedAt !== 'string') sc.submittedAt = sc.submittedAt ? String(sc.submittedAt) : null;
  if (typeof sc.submittedBy !== 'string') sc.submittedBy = sc.submittedBy ? String(sc.submittedBy) : '';
  slot.players.forEach((player) => {
    const playerKey = normalize(player.name);
    if (!sc.players[playerKey]) {
      sc.players[playerKey] = { name: player.name, holes: holeArray() };
    }
    const holes = Array.isArray(sc.players[playerKey].holes) ? sc.players[playerKey].holes.slice(0, 18) : [];
    while (holes.length < 18) holes.push(null);
    sc.players[playerKey].holes = holes.map((value) => toIntOrNull(value));
    sc.players[playerKey].name = player.name;
  });
  sc.updatedAt = new Date().toISOString();
  return { slot, scorecard: sc };
}

function getStoredScorecard(state, dayKey, slotIndex) {
  const k = keyFor(dayKey, slotIndex);
  const sc = state && state.scorecards && typeof state.scorecards === 'object' ? state.scorecards[k] : null;
  if (!sc || typeof sc !== 'object') return null;
  if (!sc.players || typeof sc.players !== 'object') sc.players = {};
  if (!sc.markers || typeof sc.markers !== 'object') sc.markers = { ctp: {}, longDrive: {} };
  if (!sc.markers.ctp || typeof sc.markers.ctp !== 'object') sc.markers.ctp = {};
  if (!sc.markers.longDrive || typeof sc.markers.longDrive !== 'object') sc.markers.longDrive = {};
  if (typeof sc.scorerName !== 'string') sc.scorerName = sc.scorerName ? String(sc.scorerName) : '';
  if (typeof sc.submittedAt !== 'string') sc.submittedAt = sc.submittedAt ? String(sc.submittedAt) : null;
  if (typeof sc.submittedBy !== 'string') sc.submittedBy = sc.submittedBy ? String(sc.submittedBy) : '';
  return sc;
}

function getDayMarkers(state, dayKey) {
  const winners = { ctp: {}, longDrive: {} };
  const winnerTs = { ctp: {}, longDrive: {} };
  getDaySlots(dayKey).forEach((slot) => {
    const scorecard = getStoredScorecard(state, dayKey, slot.slotIndex);
    if (!scorecard) return;
    const ts = Date.parse(scorecard.updatedAt || '') || 0;
    ['ctp', 'longDrive'].forEach((type) => {
      const markerMap = scorecard.markers && scorecard.markers[type] ? scorecard.markers[type] : {};
      Object.keys(markerMap).forEach((holeKey) => {
        const winner = clean(markerMap[holeKey]);
        if (!winner) return;
        if (!winnerTs[type][holeKey] || ts >= winnerTs[type][holeKey]) {
          winners[type][holeKey] = winner;
          winnerTs[type][holeKey] = ts;
        }
      });
    });
  });
  return winners;
}

function clearMarkerAssignments(state, dayKey, type, hole) {
  const holeKey = String(Number(hole));
  getDaySlots(dayKey).forEach((slot) => {
    const scorecard = getStoredScorecard(state, dayKey, slot.slotIndex);
    if (!scorecard || !scorecard.markers || !scorecard.markers[type]) return;
    delete scorecard.markers[type][holeKey];
  });
}

function assertScorecardEditable(scorecard, allowSubmittedEdit = false) {
  if (scorecard && scorecard.submittedAt && allowSubmittedEdit !== true) {
    const error = new Error('Scorecard already submitted. Admin code required to edit.');
    error.status = 403;
    throw error;
  }
}

function setSlotCode(state, dayKey, slotIndex, code) {
  const slot = getDaySlots(dayKey).find((item) => item.slotIndex === Number(slotIndex));
  if (!slot) throw new Error('Tee time slot not found for this day.');
  const rawCode = clean(code).toUpperCase() || generateCode();
  const salt = makeSalt();
  state.codes[keyFor(dayKey, slotIndex)] = {
    salt,
    hash: hashCode(salt, rawCode),
    updatedAt: new Date().toISOString(),
  };
  ensureSlotScorecard(state, dayKey, slotIndex);
  return { code: rawCode, slot };
}

function verifySlotCode(state, dayKey, slotIndex, code) {
  if (!(state.settings && state.settings.enableFoursomeCodes)) return true;
  const rec = state.codes[keyFor(dayKey, slotIndex)];
  if (!rec || !rec.salt || !rec.hash) return false;
  const probe = hashCode(rec.salt, code);
  return crypto.timingSafeEqual(Buffer.from(rec.hash, 'hex'), Buffer.from(probe, 'hex'));
}

function getDayHandicapMap(dayKey) {
  if (clean(dayKey) === PRACTICE_DAY_KEY) return new Map();
  const map = new Map();
  for (const group of FOURSOMES) {
    const dayPlayers = (group.playersByDay && group.playersByDay[dayKey]) || [];
    dayPlayers.forEach((player) => {
      const key = normalize(player.name);
      if (!map.has(key)) map.set(key, Number(player.hcp) || 0);
    });
  }
  return map;
}

function getAllowedMarkerHoles(type = '') {
  if (type === 'ctp') return SEED_MARKER_HOLES.ctp.slice();
  return ALL_HOLES.slice();
}

function getDayHoleStrokeIndex(dayKey = '', holeNumber = 0) {
  const row = DAY_HOLE_STROKE_INDEX[clean(dayKey)] || null;
  if (!row || !Number.isInteger(holeNumber) || holeNumber < 1 || holeNumber > 18) return null;
  return toIntOrNull(row[holeNumber - 1]);
}

function getHoleStrokeAllowance(hcp, holeNumber, dayKey = '') {
  const playing = Math.max(0, Math.round(Number(hcp) || 0));
  if (!playing) return 0;
  const base = Math.floor(playing / 18);
  const extra = playing % 18;
  const strokeIndex = getDayHoleStrokeIndex(dayKey, holeNumber) || holeNumber;
  return base + (strokeIndex <= extra ? 1 : 0);
}

function summarizePlayerCard(playerCard, hcp, dayKey = '') {
  const holes = Array.isArray(playerCard && playerCard.holes) ? playerCard.holes.slice(0, 18) : holeArray();
  while (holes.length < 18) holes.push(null);
  let grossTotal = 0;
  let netTotal = 0;
  let frontNet = 0;
  let backNet = 0;
  let frontPlayed = 0;
  let backPlayed = 0;
  let complete18 = true;
  let completeFront = true;
  let completeBack = true;
  for (let i = 0; i < 18; i += 1) {
    const gross = toIntOrNull(holes[i]);
    const holeNo = i + 1;
    if (gross === null) {
      complete18 = false;
      if (holeNo <= 9) completeFront = false;
      else completeBack = false;
      continue;
    }
    const strokes = getHoleStrokeAllowance(hcp, holeNo, dayKey);
    const net = gross - strokes;
    grossTotal += gross;
    netTotal += net;
    if (holeNo <= 9) {
      frontNet += net;
      frontPlayed += 1;
    } else {
      backNet += net;
      backPlayed += 1;
    }
  }
  return {
    holes,
    grossTotal: complete18 ? grossTotal : null,
    netTotal: complete18 ? netTotal : null,
    frontNet: completeFront ? frontNet : null,
    backNet: completeBack ? backNet : null,
    frontPlayed,
    backPlayed,
    completeFront,
    completeBack,
    complete18,
  };
}

function getDayPlayerSummaries(state, dayKey) {
  const handicapMap = getDayHandicapMap(dayKey);
  const out = new Map();
  getDaySlots(dayKey).forEach((slot) => {
    const k = keyFor(dayKey, slot.slotIndex);
    const card = state.scorecards[k] || {};
    const cardPlayers = (card && card.players) || {};
    slot.players.forEach((player) => {
      const nameKey = normalize(player.name);
      const src = cardPlayers[nameKey] || { name: player.name, holes: holeArray() };
      const penalties = getPenaltyEntry(state, player.name);
      const summary = summarizePlayerCard(src, handicapMap.get(nameKey) || 0, dayKey);
      out.set(nameKey, {
        name: player.name,
        hcp: handicapMap.get(nameKey) || 0,
        ...summary,
        penaltyChampion: penalties.champion,
        penaltyRookie: penalties.rookie,
        penaltyTotal: penalties.total,
        adjustedNetTotal: summary.netTotal === null ? null : Number((summary.netTotal + penalties.total).toFixed(2)),
      });
    });
  });
  return out;
}

function getDayMatrixByPlayer(dayKey) {
  if (clean(dayKey) === PRACTICE_DAY_KEY) return new Map();
  const rules = [{ pairs: [[0, 1], [2, 3]] }, { pairs: [[0, 2], [1, 3]] }, { pairs: [[0, 3], [1, 2]] }];
  const rows = [];
  FOURSOMES.forEach((group) => {
    const players = (group.playersByDay && group.playersByDay[dayKey]) || [];
    if (players.length < 4) return;
    const byPlayer = new Map(players.map((p) => [normalize(p.name), { player: p.name, segments: [] }]));
    rules.forEach((rule, segmentIndex) => {
      rule.pairs.forEach(([a, b]) => {
        const pa = players[a];
        const pb = players[b];
        byPlayer.get(normalize(pa.name)).segments.push({ opponent: pb.name, segmentIndex });
        byPlayer.get(normalize(pb.name)).segments.push({ opponent: pa.name, segmentIndex });
      });
    });
    players.forEach((p) => rows.push(byPlayer.get(normalize(p.name))));
  });
  return new Map(rows.map((row) => [normalize(row.player), row]));
}

function compareSegment(daySummaries, dayKey, playerName, opponentName, segmentIndex) {
  const me = daySummaries.get(normalize(playerName));
  const opp = daySummaries.get(normalize(opponentName));
  if (!me || !opp) return '';
  const start = segmentIndex === 0 ? 0 : 9;
  const end = segmentIndex === 0 ? 8 : 17;
  let mineShared = 0;
  let theirsShared = 0;
  let sharedHoles = 0;
  for (let idx = start; idx <= end; idx += 1) {
    const holeNo = idx + 1;
    const mineGross = toIntOrNull(me.holes[idx]);
    const theirsGross = toIntOrNull(opp.holes[idx]);
    if (mineGross === null || theirsGross === null) continue;
    mineShared += mineGross - getHoleStrokeAllowance(me.hcp, holeNo, dayKey);
    theirsShared += theirsGross - getHoleStrokeAllowance(opp.hcp, holeNo, dayKey);
    sharedHoles += 1;
  }
  if (!sharedHoles) return '';
  if (sharedHoles >= 9) {
    if (mineShared < theirsShared) return 'W';
    if (mineShared > theirsShared) return 'L';
    return 'T';
  }
  if (mineShared < theirsShared) return 'W*';
  if (mineShared > theirsShared) return 'L*';
  return 'T*';
}

function getMatchPointsFromLive(state, dayKey) {
  const matrix = getDayMatrixByPlayer(dayKey);
  const daySummaries = getDayPlayerSummaries(state, dayKey);
  const out = new Map(PLAYERS.map((p) => [normalize(p.name), 0]));
  const matchRows = [];
  matrix.forEach((row, key) => {
    let points = 0;
    const segments = (row.segments || []).map((segment) => {
      const result = compareSegment(daySummaries, dayKey, row.player, segment.opponent, segment.segmentIndex);
      if (result === 'W') points += 2;
      if (result === 'T') points += 1;
      return { opponent: segment.opponent, result, segmentIndex: segment.segmentIndex };
    });
    out.set(key, points);
    matchRows.push({ player: row.player, points, segments });
  });
  return { points: out, rows: matchRows };
}

function getStrokeBonusFromLive(state, dayKey) {
  const daySummaries = getDayPlayerSummaries(state, dayKey);
  const values = PLAYERS.map((p) => ({ key: normalize(p.name), value: toNumOrNull(daySummaries.get(normalize(p.name)) && daySummaries.get(normalize(p.name)).adjustedNetTotal) }));
  const valid = values.filter((value) => value.value !== null);
  const out = new Map(values.map((value) => [value.key, 0]));
  valid.forEach((entry) => {
    const rank = 1 + valid.filter((value) => value.value < entry.value).length;
    if (rank < 9) out.set(entry.key, 2);
  });
  return out;
}

function getDay4RankPointsFromLive(state) {
  const daySummaries = getDayPlayerSummaries(state, 'Day 4');
  const entries = PLAYERS
    .map((p) => ({ key: normalize(p.name), net: toNumOrNull(daySummaries.get(normalize(p.name)) && daySummaries.get(normalize(p.name)).adjustedNetTotal) }))
    .filter((entry) => entry.net !== null)
    .sort((a, b) => a.net - b.net);
  const pts = new Map(PLAYERS.map((p) => [normalize(p.name), 0]));
  const ranks = new Map(PLAYERS.map((p) => [normalize(p.name), null]));
  let i = 0;
  while (i < entries.length) {
    const start = i;
    const score = entries[i].net;
    while (i < entries.length && entries[i].net === score) i += 1;
    const tie = entries.slice(start, i);
    const firstRank = start + 1;
    let total = 0;
    for (let pos = firstRank; pos <= i; pos += 1) total += (TIN_CUP_RANK_POINTS[pos - 1] || 0);
    const avg = tie.length ? Number((total / tie.length).toFixed(2)) : 0;
    tie.forEach((entry) => {
      pts.set(entry.key, avg);
      ranks.set(entry.key, firstRank);
    });
  }
  return { pts, ranks };
}

function deriveWorkbookHandicap(config, handicapIndex) {
  const settings = normalizeWorkbookConfig(config);
  const index = toNumOrNull(handicapIndex) || 0;
  const converted = Number((index * (1 + settings.handicap.indexToHcConversion)).toFixed(2));
  const eighteenHole = Math.min(settings.handicap.maxHandicap, Math.round(converted));
  const nineHole = Math.min(settings.handicap.maxHandicap * 0.5, Math.round(eighteenHole / 2));
  const par3 = Math.round(eighteenHole * settings.handicap.par3Fraction);
  return {
    handicapIndex: index,
    converted,
    eighteenHole,
    nineHole,
    par3,
  };
}

function buildHandicapSummary(state) {
  const config = state.config || defaultWorkbookConfig();
  return PLAYERS.map((player) => {
    const derived = deriveWorkbookHandicap(config, player.handicap);
    const penalty = getPenaltyEntry(state, player.name);
    return {
      name: player.name,
      handicapIndex: player.handicap,
      convertedHandicap: derived.converted,
      eighteenHole: derived.eighteenHole,
      nineHole: derived.nineHole,
      par3: derived.par3,
      championPenalty: penalty.champion,
      rookiePenalty: penalty.rookie,
      totalPenalty: penalty.total,
    };
  });
}

function getScrambleTeams() {
  return getDaySlots('Scramble').map((slot) => ({
    teamIndex: slot.slotIndex,
    key: `team${slot.slotIndex + 1}`,
    label: `Team ${slot.slotIndex + 1}`,
    players: slot.players.map((player) => player.name),
  }));
}

function ensureScrambleTeamScores(state, teamIndex) {
  if (!state.scramble || typeof state.scramble !== 'object') state.scramble = normalizeScrambleState();
  if (!state.scramble.scores || typeof state.scramble.scores !== 'object') state.scramble.scores = {};
  const key = `team${Number(teamIndex) + 1}`;
  if (!state.scramble.scores[key]) state.scramble.scores[key] = holeScoreArray();
  state.scramble.scores[key] = holeScoreArray(state.scramble.scores[key]);
  return { key, holes: state.scramble.scores[key] };
}

function updateScrambleHoleScore(state, payload = {}) {
  const teamIndex = Number(payload.teamIndex);
  const hole = Number(payload.hole);
  if (!Number.isInteger(teamIndex) || teamIndex < 0) throw new Error('teamIndex required');
  if (!Number.isInteger(hole) || hole < 1 || hole > 18) throw new Error('hole must be 1-18');
  const team = getScrambleTeams().find((entry) => entry.teamIndex === teamIndex);
  if (!team) throw new Error('Scramble team not found');
  const { holes } = ensureScrambleTeamScores(state, teamIndex);
  holes[hole - 1] = toIntOrNull(payload.gross);
  return getScrambleResults(state);
}

function getScrambleResults(state) {
  const config = normalizeWorkbookConfig(state && state.config ? state.config : defaultWorkbookConfig());
  const scramblePoints = Array.isArray(config.accounting && config.accounting.scramblePoints)
    ? config.accounting.scramblePoints
    : SCRAMBLE_POINTS;
  const teams = getScrambleTeams().map((team) => {
    const { holes } = ensureScrambleTeamScores(state, team.teamIndex);
    const played = holes.filter((value) => value !== null).length;
    const total = played === 18 ? holes.reduce((sum, value) => sum + (value || 0), 0) : null;
    return {
      ...team,
      holes: holeScoreArray(holes),
      played,
      total,
      rank: null,
      points: 0,
    };
  });
  const complete = teams.filter((team) => team.total !== null).sort((a, b) => a.total - b.total || a.teamIndex - b.teamIndex);
  let idx = 0;
  while (idx < complete.length) {
    const start = idx;
    const score = complete[idx].total;
    while (idx < complete.length && complete[idx].total === score) idx += 1;
    const firstRank = start + 1;
    let totalPoints = 0;
    for (let pos = firstRank; pos <= idx; pos += 1) totalPoints += toNumOrNull(scramblePoints[pos - 1]) || 0;
    const avgPoints = idx > start ? Number((totalPoints / (idx - start)).toFixed(2)) : 0;
    complete.slice(start, idx).forEach((team) => {
      team.rank = firstRank;
      team.points = avgPoints;
    });
  }
  const pointsByPlayer = new Map(PLAYERS.map((player) => [normalize(player.name), 0]));
  teams.forEach((team) => {
    team.players.forEach((name) => {
      pointsByPlayer.set(normalize(name), team.points || 0);
    });
  });
  return {
    teams,
    hasScores: teams.some((team) => team.played > 0),
    pointsByPlayer,
  };
}

function getScramblePointsByPlayer(state) {
  const scramble = getScrambleResults(state);
  if (scramble.hasScores) return scramble.pointsByPlayer;
  return new Map(PLAYERS.map((player) => [normalize(player.name), toNumOrNull((state.scrambleBonus || {})[normalize(player.name)]) || 0]));
}

function buildScoreRankings(state) {
  const config = state.config || defaultWorkbookConfig();
  const competitiveDays = ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4'];
  const dayMaps = new Map(competitiveDays.map((dayKey) => [dayKey, getDayPlayerSummaries(state, dayKey)]));
  const rows = PLAYERS.map((player) => {
    const key = normalize(player.name);
    const rounds = competitiveDays.map((dayKey) => dayMaps.get(dayKey).get(key)).filter((summary) => summary && summary.grossTotal !== null);
    const grossValues = rounds.map((summary) => summary.grossTotal);
    const averageGross = grossValues.length ? Number((grossValues.reduce((sum, value) => sum + value, 0) / grossValues.length).toFixed(2)) : null;
    const derived = deriveWorkbookHandicap(config, player.handicap);
    const netScore = averageGross === null ? null : Number((averageGross - derived.eighteenHole).toFixed(2));
    return {
      name: player.name,
      roundsPlayed: grossValues.length,
      averageGross,
      handicap18: derived.eighteenHole,
      netScore,
      position: null,
      label: '',
    };
  }).filter((row) => row.averageGross !== null)
    .sort((a, b) => (a.netScore - b.netScore) || (a.averageGross - b.averageGross) || a.name.localeCompare(b.name));
  rows.forEach((row, index) => {
    row.position = index + 1;
    row.label = row.position === 1 ? 'Winner' : row.position === 2 ? '2nd' : row.position === 3 ? '3rd' : '';
  });
  return rows;
}

function getSkinsDayDefinitions() {
  return [
    { dayKey: 'Day 1', sourceDays: ['Day 1'], payoutEligible: true },
    { dayKey: 'Day 2', sourceDays: ['Day 2A', 'Day 2B'], payoutEligible: true },
    { dayKey: 'Day 3', sourceDays: ['Day 3'], payoutEligible: true },
    { dayKey: 'Day 4', sourceDays: ['Day 4'], payoutEligible: true },
    { dayKey: 'Practice', sourceDays: ['Practice'], payoutEligible: false },
  ];
}

function buildSkinsForDay(state, definition) {
  const src = definition || {};
  const sourceDays = Array.isArray(src.sourceDays) && src.sourceDays.length ? src.sourceDays : [src.dayKey];
  const summaryMaps = sourceDays.map((dayKey) => ({ dayKey, summaries: getDayPlayerSummaries(state, dayKey) }));
  const holes = ALL_HOLES.map((holeNo) => {
    const sourceIndex = sourceDays.length > 1 && holeNo > 9 ? 1 : 0;
    const source = summaryMaps[sourceIndex] || summaryMaps[0];
    const entries = PLAYERS.map((player) => {
      const summary = source.summaries.get(normalize(player.name));
      if (!summary) return null;
      const gross = toIntOrNull(summary.holes[holeNo - 1]);
      if (gross === null) return null;
      return {
        name: player.name,
        net: gross - getHoleStrokeAllowance(summary.hcp, holeNo, source.dayKey),
      };
    }).filter(Boolean).sort((a, b) => a.net - b.net || a.name.localeCompare(b.name));
    if (!entries.length) return { hole: holeNo, winner: '', net: null, hasSkin: false };
    const low = entries[0].net;
    const tied = entries.filter((entry) => entry.net === low);
    if (tied.length !== 1) return { hole: holeNo, winner: '', net: low, hasSkin: false };
    return { hole: holeNo, winner: tied[0].name, net: low, hasSkin: true };
  });
  const skinCount = holes.filter((hole) => hole.hasSkin).length;
  const pot = src.payoutEligible === false
    ? 0
    : toDollar(Number((((state.config || defaultWorkbookConfig()).accounting || {}).skins || {}).perPerson || 0) * PLAYERS.length);
  const payoutPerSkin = skinCount && pot ? toDollar(pot / skinCount) : 0;
  const payouts = PLAYERS.reduce((acc, player) => {
    acc[normalize(player.name)] = 0;
    return acc;
  }, {});
  holes.forEach((hole) => {
    if (!hole.hasSkin || !hole.winner || !payoutPerSkin) return;
    payouts[normalize(hole.winner)] = toDollar(payouts[normalize(hole.winner)] + payoutPerSkin);
  });
  const winners = PLAYERS.map((player) => ({
    name: player.name,
    skins: holes.filter((hole) => normalize(hole.winner) === normalize(player.name)).length,
    payout: payouts[normalize(player.name)] || 0,
  })).filter((row) => row.skins > 0 || row.payout > 0);
  return { dayKey: src.dayKey || sourceDays[0], sourceDays, payoutEligible: src.payoutEligible !== false, pot, payoutPerSkin, skinCount, holes, winners };
}

function buildSkinsResults(state) {
  const days = getSkinsDayDefinitions().map((definition) => buildSkinsForDay(state, definition));
  const totals = PLAYERS.map((player) => ({ name: player.name, skins: 0, payout: 0 }));
  const byKey = new Map(totals.map((row) => [normalize(row.name), row]));
  days.filter((day) => day.payoutEligible !== false).forEach((day) => {
    day.winners.forEach((winner) => {
      const row = byKey.get(normalize(winner.name));
      row.skins += winner.skins;
      row.payout = toDollar(row.payout + winner.payout);
    });
  });
  return {
    days,
    totals: totals.filter((row) => row.skins > 0 || row.payout > 0).sort((a, b) => (b.payout - a.payout) || (b.skins - a.skins) || a.name.localeCompare(b.name)),
  };
}

function getFinalPayouts(rows, payoutValues = []) {
  const out = new Map(PLAYERS.map((player) => [normalize(player.name), 0]));
  let index = 0;
  while (index < rows.length && index < payoutValues.length) {
    const start = index;
    const position = rows[index].position;
    while (index < rows.length && rows[index].position === position && index < payoutValues.length) index += 1;
    const total = payoutValues.slice(start, index).reduce((sum, value) => sum + (toNumOrNull(value) || 0), 0);
    const avg = index > start ? toDollar(total / (index - start)) : 0;
    rows.slice(start, index).forEach((row) => out.set(normalize(row.name), avg));
  }
  return out;
}

function buildLeftoverSuggestion(balance) {
  const dollars = toDollar(balance);
  if (dollars <= 0) return '';
  if (dollars <= 10) return `Use the remaining $${dollars} as a tip add-on or cash tiebreak bonus.`;
  if (dollars <= 25) return `Use the remaining $${dollars} for drinks or a closest-to-the-pin bonus.`;
  if (dollars <= 50) return `Use the remaining $${dollars} for a final-hole side prize or drinks.`;
  return `Use the remaining $${dollars} for drinks, dinner, or roll it into next year's side-game pot.`;
}

function getMarkerPayoutCounts(state) {
  const counts = {
    ctp: new Map(PLAYERS.map((player) => [normalize(player.name), 0])),
    longDrive: new Map(PLAYERS.map((player) => [normalize(player.name), 0])),
  };
  ['Day 1', 'Day 2A', 'Day 2B', 'Day 3', 'Day 4'].forEach((dayKey) => {
    const markers = getDayMarkers(state, dayKey);
    ['ctp', 'longDrive'].forEach((type) => {
      Object.values(markers[type] || {}).forEach((winner) => {
        const key = normalize(winner);
        counts[type].set(key, (counts[type].get(key) || 0) + 1);
      });
    });
  });
  return counts;
}

function buildPayoutSummary(state, leaderboard = null) {
  const board = leaderboard || buildLeaderboard(state);
  const config = normalizeWorkbookConfig(state.config || defaultWorkbookConfig());
  const mainPot = toDollar((config.accounting.entryFee || 0) * PLAYERS.length);
  const skins = buildSkinsResults(state);
  const sideGames = buildSideGameSummary(state);
  const skinsPot = toDollar((skins.days || []).filter((day) => day.payoutEligible !== false).reduce((sum, day) => sum + Number(day.pot || 0), 0));
  const combinedPot = toDollar(mainPot + skinsPot);
  const finalPayouts = getFinalPayouts(board.totals || [], config.accounting.finalPayouts || []);
  const markerCounts = getMarkerPayoutCounts(state);
  const loserRow = (board.totals || []).slice().sort((a, b) => (a.total - b.total) || b.name.localeCompare(a.name))[0] || null;
  const rows = PLAYERS.map((player) => {
    const key = normalize(player.name);
    const finalPrize = finalPayouts.get(key) || 0;
    const ctp = toDollar((markerCounts.ctp.get(key) || 0) * (config.accounting.markerPayouts.ctp || 0));
    const longDrive = toDollar((markerCounts.longDrive.get(key) || 0) * (config.accounting.markerPayouts.longDrive || 0));
    const longPuttWins = ((((sideGames.longPutt || {}).totals) || []).find((row) => normalize(row.name) === key) || {}).wins || 0;
    const secretSnowmanWins = ((((sideGames.secretSnowman || {}).totals) || []).find((row) => normalize(row.name) === key) || {}).wins || 0;
    const longPutt = toDollar(longPuttWins * (config.accounting.markerPayouts.longPutt || 0));
    const secretSnowman = toDollar(secretSnowmanWins * (config.accounting.markerPayouts.secretSnowman || 0));
    const skinsRow = (skins.totals || []).find((row) => normalize(row.name) === key);
    const loser = loserRow && normalize(loserRow.name) === key ? toDollar(config.accounting.markerPayouts.loser || 0) : 0;
    const skinsPayout = toDollar(skinsRow ? skinsRow.payout : 0);
    const total = toDollar(finalPrize + ctp + longDrive + longPutt + secretSnowman + skinsPayout + loser);
    return {
      name: player.name,
      finalPrize,
      ctp,
      longDrive,
      longPutt,
      secretSnowman,
      skins: skinsPayout,
      loser,
      total,
    };
  });
  const distributed = toDollar(rows.reduce((sum, row) => sum + row.total, 0));
  const balance = toDollar(combinedPot - distributed);
  return {
    pot: combinedPot,
    mainPot,
    skinsPot,
    distributed,
    balance,
    leftoverSuggestion: buildLeftoverSuggestion(balance),
    rows: rows.filter((row) => row.total > 0).sort((a, b) => (b.total - a.total) || a.name.localeCompare(b.name)),
  };
}

function buildSeedSummary(state, leaderboard = null) {
  const board = leaderboard || buildLeaderboard(state);
  const payouts = board.payouts && Array.isArray(board.payouts.rows) ? board.payouts : buildPayoutSummary(state, board);
  const sideGames = board.sideGames || buildSideGameSummary(state);
  const scramble = board.scramble || getScrambleResults(state);
  const skins = board.skins || buildSkinsResults(state);
  const markerCounts = getMarkerPayoutCounts(state);
  const winsByPlayer = (map = new Map()) => PLAYERS.map((player) => ({
    name: player.name,
    wins: map.get(normalize(player.name)) || 0,
  })).filter((row) => row.wins > 0)
    .sort((a, b) => (b.wins - a.wins) || a.name.localeCompare(b.name));
  const loserRow = (payouts.rows || []).find((row) => Number(row.loser || 0) > 0) || null;
  return {
    longPutt: sideGames.longPutt || { label: SIDE_GAME_DEFS.longPutt.label, days: [], totals: [] },
    secretSnowman: sideGames.secretSnowman || { label: SIDE_GAME_DEFS.secretSnowman.label, days: [], totals: [] },
    markerTotals: {
      ctp: winsByPlayer(markerCounts.ctp),
      longDrive: winsByPlayer(markerCounts.longDrive),
    },
    scramble: {
      teams: (scramble.teams || []).map((team) => ({
        label: team.label,
        players: Array.isArray(team.players) ? team.players.slice() : [],
        total: team.total,
        rank: team.rank,
        points: team.points || 0,
      })),
    },
    skins: {
      totals: Array.isArray(skins.totals) ? skins.totals.slice() : [],
    },
    loser: loserRow ? { name: loserRow.name, payout: Number(loserRow.loser || 0) } : null,
    payoutRows: (payouts.rows || [])
      .filter((row) => Number(row.ctp || 0) > 0
        || Number(row.longDrive || 0) > 0
        || Number(row.longPutt || 0) > 0
        || Number(row.secretSnowman || 0) > 0
        || Number(row.skins || 0) > 0
        || Number(row.loser || 0) > 0)
      .map((row) => ({
        name: row.name,
        ctp: Number(row.ctp || 0),
        longDrive: Number(row.longDrive || 0),
        longPutt: Number(row.longPutt || 0),
        secretSnowman: Number(row.secretSnowman || 0),
        skins: Number(row.skins || 0),
        loser: Number(row.loser || 0),
        total: Number(row.total || 0),
      })),
    competitionDays: COMPETITIVE_DAYS.slice(),
  };
}


function buildWorkbookResultsAudit(state, leaderboard = null) {
  const board = leaderboard || buildLeaderboard(state);
  const rankings = Array.isArray(board.scoreRankings) ? board.scoreRankings : buildScoreRankings(state);
  const payouts = board.payouts && Array.isArray(board.payouts.rows) ? board.payouts : buildPayoutSummary(state, board);
  const skins = board.skins || buildSkinsResults(state);
  const scramble = board.scramble || getScrambleResults(state);
  const sideGames = board.sideGames || buildSideGameSummary(state);
  const rankingByName = new Map(rankings.map((row) => [normalize(row.name), row]));
  const payoutByName = new Map((payouts.rows || []).map((row) => [normalize(row.name), row]));
  const skinsByName = new Map((skins.totals || []).map((row) => [normalize(row.name), row]));
  const longPuttByName = new Map((((sideGames.longPutt || {}).totals) || []).map((row) => [normalize(row.name), row]));
  const secretSnowmanByName = new Map((((sideGames.secretSnowman || {}).totals) || []).map((row) => [normalize(row.name), row]));
  const markerCounts = getMarkerPayoutCounts(state);
  const loserSet = new Set((payouts.rows || []).filter((row) => Number(row.loser || 0) > 0).map((row) => normalize(row.name)));
  const scrambleByName = new Map();
  (scramble.teams || []).forEach((team) => {
    (team.players || []).forEach((name) => {
      scrambleByName.set(normalize(name), {
        label: team.label,
        total: team.total,
        rank: team.rank,
        points: team.points || 0,
      });
    });
  });
  return (board.totals || []).map((row) => {
    const key = normalize(row.name);
    const rank = rankingByName.get(key) || {};
    const payout = payoutByName.get(key) || {};
    const skin = skinsByName.get(key) || {};
    const longPutt = longPuttByName.get(key) || {};
    const secretSnowman = secretSnowmanByName.get(key) || {};
    const scrambleRow = scrambleByName.get(key) || {};
    return {
      name: row.name,
      tripPosition: row.position,
      tripTotal: row.total,
      averageGross: rank.averageGross ?? null,
      netScore: rank.netScore ?? null,
      netRank: rank.position ?? null,
      scoreLabel: rank.label || '',
      scrambleTeam: scrambleRow.label || '',
      scrambleTotal: scrambleRow.total ?? null,
      scrambleRank: scrambleRow.rank ?? null,
      scramblePoints: scrambleRow.points ?? row.scramble ?? 0,
      skinsWon: skin.skins || 0,
      skinsPayout: skin.payout || 0,
      ctpWins: markerCounts.ctp.get(key) || 0,
      longDriveWins: markerCounts.longDrive.get(key) || 0,
      longPuttWins: longPutt.wins || 0,
      secretSnowmanWins: secretSnowman.wins || 0,
      loser: loserSet.has(key),
      finalPrize: payout.finalPrize || 0,
      payoutTotal: payout.total || 0,
    };
  }).sort((a, b) => (a.tripPosition - b.tripPosition) || a.name.localeCompare(b.name));
}

function toIsoText(value) {
  if (!value) return '';
  const dt = value instanceof Date ? value : new Date(value);
  return Number.isNaN(dt.getTime()) ? '' : dt.toISOString();
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (Array.isArray(value)) return value.join(' | ');
  return String(value);
}

function buildCsvText(rows = []) {
  const preferred = [
    'rowType',
    'tripId',
    'tripName',
    'exportedAt',
    'tripStartDate',
    'tripEndDate',
    'leaderboardGeneratedAt',
    'dayKey',
    'slotIndex',
    'slotLabel',
    'playerName',
    'teamIndex',
    'teamLabel',
    'type',
    'hole',
    'winner',
    'position',
    'points',
    'tripTotal',
    'total',
    'payoutTotal',
  ];
  const headers = [];
  rows.forEach((row) => {
    Object.keys(row || {}).forEach((key) => {
      if (!headers.includes(key)) headers.push(key);
    });
  });
  const orderedHeaders = [
    ...preferred.filter((key) => headers.includes(key)),
    ...headers.filter((key) => !preferred.includes(key)),
  ];
  if (!orderedHeaders.length) return '';
  const escape = (value) => `"${csvCell(value).replace(/"/g, '""')}"`;
  return [
    orderedHeaders.map(escape).join(','),
    ...rows.map((row) => orderedHeaders.map((key) => escape(row && Object.prototype.hasOwnProperty.call(row, key) ? row[key] : '')).join(',')),
  ].join('\n');
}

function buildCompetitionExportRows(state, options = {}) {
  const board = buildLeaderboard(state);
  board.payouts = buildPayoutSummary(state, board);
  const settings = normalizeSettings(state && state.settings ? state.settings : {}, defaultTinCupLiveState().settings);
  const config = normalizeWorkbookConfig(state && state.config ? state.config : defaultWorkbookConfig());
  const exportedAt = toIsoText(options.exportedAt) || new Date().toISOString();
  const base = {
    tripId: clean(options.tripId),
    tripName: clean(options.tripName) || 'Tin Cup 2026',
    exportedAt,
    tripStartDate: toIsoText(options.tripStartDate),
    tripEndDate: toIsoText(options.tripEndDate),
    leaderboardGeneratedAt: clean(board.generatedAt),
  };
  const workbookByName = new Map((board.workbookResults || []).map((row) => [normalize(row.name), row]));
  const handicapByName = new Map((board.handicapSummary || []).map((row) => [normalize(row.name), row]));
  const payoutByName = new Map((((board.payouts || {}).rows) || []).map((row) => [normalize(row.name), row]));
  const practiceByName = new Map((((board.matchBoards || {}).Practice) || []).map((row) => [normalize(row.player), row]));
  const rows = [{
    ...base,
    rowType: 'trip_summary',
    playerCount: PLAYERS.length,
    matchDayCount: MATCH_DAY_OPTIONS.length,
    competitiveDayCount: COMPETITIVE_DAYS.length,
    scorecardCount: Object.keys((state && state.scorecards && typeof state.scorecards === 'object') ? state.scorecards : {}).length,
    scrambleTeamCount: Array.isArray(board.scramble && board.scramble.teams) ? board.scramble.teams.length : 0,
    combinedPot: Number(((board.payouts || {}).pot) || 0),
    entryPot: Number(((board.payouts || {}).mainPot) || 0),
    skinsPot: Number(((board.payouts || {}).skinsPot) || 0),
    distributed: Number(((board.payouts || {}).distributed) || 0),
    balance: Number(((board.payouts || {}).balance) || 0),
  }, {
    ...base,
    rowType: 'config',
    enableLiveFoursomeScoring: settings.enableLiveFoursomeScoring === true,
    enableFoursomeCodes: settings.enableFoursomeCodes === true,
    enableLiveMarkers: settings.enableLiveMarkers === true,
    enableLiveLeaderboard: settings.enableLiveLeaderboard === true,
    handicapIndexToHcConversion: config.handicap.indexToHcConversion,
    averageCourseSlope: config.handicap.avgCourseSlope,
    averageSlope: config.handicap.avgSlope,
    maxHandicap: config.handicap.maxHandicap,
    par3Fraction: config.handicap.par3Fraction,
    entryFee: config.accounting.entryFee,
    finalPayout1: config.accounting.finalPayouts[0] || 0,
    finalPayout2: config.accounting.finalPayouts[1] || 0,
    finalPayout3: config.accounting.finalPayouts[2] || 0,
    finalPayout4: config.accounting.finalPayouts[3] || 0,
    markerPayoutLongDrive: config.accounting.markerPayouts.longDrive,
    markerPayoutCtp: config.accounting.markerPayouts.ctp,
    markerPayoutLongPutt: config.accounting.markerPayouts.longPutt,
    markerPayoutSecretSnowman: config.accounting.markerPayouts.secretSnowman,
    markerPayoutLoser: config.accounting.markerPayouts.loser,
    skinsPerPerson: config.accounting.skins.perPerson,
    scramblePoints1: config.accounting.scramblePoints[0] || 0,
    scramblePoints2: config.accounting.scramblePoints[1] || 0,
    scramblePoints3: config.accounting.scramblePoints[2] || 0,
    scramblePoints4: config.accounting.scramblePoints[3] || 0,
  }];

  (board.totals || []).forEach((row) => {
    const key = normalize(row.name);
    const workbook = workbookByName.get(key) || {};
    const handicap = handicapByName.get(key) || {};
    const payout = payoutByName.get(key) || {};
    const practice = practiceByName.get(key) || {};
    rows.push({
      ...base,
      rowType: 'player_total',
      playerName: row.name,
      position: row.position,
      tripTotal: row.total,
      day1MatchPoints: row.match1,
      day2AMatchPoints: row.match2A,
      day2BMatchPoints: row.match2B,
      day2TotalMatchPoints: row.match2,
      day3MatchPoints: row.match3,
      practiceMatchPoints: practice.points || 0,
      day1StrokeBonus: row.stroke1,
      day3StrokeBonus: row.stroke3,
      scramblePoints: row.scramble,
      day4RankPoints: row.day4Points,
      day4Rank: row.day4Rank,
      day1TotalPoints: row.day1Total,
      day2TotalPoints: row.day2Total,
      day3TotalPoints: row.day3Total,
      day1Net: row.day1Net,
      day3Net: row.day3Net,
      day4Net: row.day4Net,
      penaltyChampion: row.penaltyChampion,
      penaltyRookie: row.penaltyRookie,
      penaltyTotal: row.penaltyTotal,
      handicapIndex: handicap.handicapIndex ?? null,
      convertedHandicap: handicap.convertedHandicap ?? null,
      nineHoleHandicap: handicap.nineHole ?? null,
      eighteenHoleHandicap: handicap.eighteenHole ?? null,
      par3Handicap: handicap.par3 ?? null,
      averageGross: workbook.averageGross ?? null,
      netScore: workbook.netScore ?? null,
      netRank: workbook.netRank ?? null,
      scoreLabel: workbook.scoreLabel || '',
      scrambleTeam: workbook.scrambleTeam || '',
      scrambleGrossTotal: workbook.scrambleTotal ?? null,
      scrambleRank: workbook.scrambleRank ?? null,
      skinsWon: workbook.skinsWon || 0,
      skinsPayout: workbook.skinsPayout || 0,
      ctpWins: workbook.ctpWins || 0,
      longDriveWins: workbook.longDriveWins || 0,
      longPuttWins: workbook.longPuttWins || 0,
      secretSnowmanWins: workbook.secretSnowmanWins || 0,
      loserFlag: workbook.loser === true,
      finalPrize: payout.finalPrize || 0,
      ctpPayout: payout.ctp || 0,
      longDrivePayout: payout.longDrive || 0,
      longPuttPayout: payout.longPutt || 0,
      secretSnowmanPayout: payout.secretSnowman || 0,
      loserPayout: payout.loser || 0,
      payoutTotal: payout.total || 0,
    });
  });

  Object.entries(board.matchBoards || {}).forEach(([dayKey, matchRows]) => {
    (matchRows || []).forEach((row) => {
      const segments = Array.isArray(row.segments) ? row.segments : [];
      rows.push({
        ...base,
        rowType: 'match_day',
        dayKey,
        playerName: row.player,
        points: row.points,
        segment1Opponent: (segments[0] || {}).opponent || '',
        segment1Result: (segments[0] || {}).result || '',
        segment2Opponent: (segments[1] || {}).opponent || '',
        segment2Result: (segments[1] || {}).result || '',
        segment3Opponent: (segments[2] || {}).opponent || '',
        segment3Result: (segments[2] || {}).result || '',
      });
    });
  });

  MATCH_DAY_OPTIONS.forEach((dayKey) => {
    getDaySlots(dayKey).forEach((slot) => {
      const scorecard = getStoredScorecard(state, dayKey, slot.slotIndex);
      const scorerName = clean(scorecard && scorecard.scorerName);
      const submittedAt = clean(scorecard && scorecard.submittedAt);
      const submittedBy = clean(scorecard && scorecard.submittedBy);
      const updatedAt = clean(scorecard && scorecard.updatedAt);
      slot.players.forEach((player) => {
        const entry = scorecard && scorecard.players ? (scorecard.players[normalize(player.name)] || { name: player.name, holes: holeArray() }) : { name: player.name, holes: holeArray() };
        const summary = summarizePlayerCard(entry, player.hcp, dayKey);
        const penalty = getPenaltyEntry(state, player.name);
        const holeValues = {};
        summary.holes.forEach((gross, index) => {
          holeValues[`hole${index + 1}`] = gross;
        });
        rows.push({
          ...base,
          rowType: 'scorecard_player',
          dayKey,
          slotIndex: slot.slotIndex,
          slotLabel: slot.label,
          playerName: player.name,
          scorerName,
          submitted: Boolean(submittedAt),
          submittedAt,
          submittedBy,
          updatedAt,
          handicap: player.hcp,
          grossTotal: summary.grossTotal,
          netTotal: summary.netTotal,
          penaltyChampion: penalty.champion,
          penaltyRookie: penalty.rookie,
          penaltyTotal: penalty.total,
          adjustedNetTotal: summary.netTotal === null ? null : Number((summary.netTotal + penalty.total).toFixed(2)),
          complete18: summary.complete18 === true,
          ...holeValues,
        });
      });
    });
  });

  MATCH_DAY_OPTIONS.forEach((dayKey) => {
    const markers = getDayMarkers(state, dayKey);
    ['ctp', 'longDrive'].forEach((type) => {
      Object.entries((markers && markers[type]) || {}).forEach(([holeKey, winner]) => {
        rows.push({
          ...base,
          rowType: 'marker',
          dayKey,
          type,
          hole: Number(holeKey),
          winner,
        });
      });
    });
  });

  Object.entries(board.sideGames || {}).forEach(([type, summary]) => {
    ((summary && summary.days) || []).forEach((dayRow) => {
      rows.push({
        ...base,
        rowType: 'side_game',
        type,
        dayKey: dayRow.dayKey,
        winner: dayRow.winner || '',
      });
    });
  });

  ((board.scramble || {}).teams || []).forEach((team) => {
    const holeValues = {};
    (team.holes || []).forEach((gross, index) => {
      holeValues[`hole${index + 1}`] = gross;
    });
    rows.push({
      ...base,
      rowType: 'scramble_team',
      teamIndex: team.teamIndex,
      teamLabel: team.label,
      players: Array.isArray(team.players) ? team.players.join(' | ') : '',
      playedHoles: team.played,
      total: team.total,
      rank: team.rank,
      points: team.points || 0,
      ...holeValues,
    });
  });

  ((board.skins || {}).days || []).forEach((day) => {
    (day.holes || []).forEach((hole) => {
      rows.push({
        ...base,
        rowType: 'skin_hole',
        dayKey: day.dayKey,
        hole: hole.hole,
        winner: hole.winner || '',
        net: hole.net,
        hasSkin: hole.hasSkin === true,
        payoutEligible: day.payoutEligible !== false,
        pot: day.pot || 0,
        payoutPerSkin: day.payoutPerSkin || 0,
      });
    });
  });

  return rows;
}

function buildCompetitionExportCsv(state, options = {}) {
  return buildCsvText(buildCompetitionExportRows(state, options));
}

function buildMatchDetailBoards(state) {
  const labels = ['Front 9', 'Back 9 #1', 'Back 9 #2'];
  const rules = [{ pairs: [[0, 1], [2, 3]] }, { pairs: [[0, 2], [1, 3]] }, { pairs: [[0, 3], [1, 2]] }];
  return MATCH_DAY_OPTIONS.reduce((acc, dayKey) => {
    const summaries = getDayPlayerSummaries(state, dayKey);
    acc[dayKey] = getDaySlots(dayKey).map((slot) => ({
      slotIndex: slot.slotIndex,
      label: slot.label,
      players: slot.players.map((player) => player.name),
      segments: rules.map((rule, segmentIndex) => ({
        label: labels[segmentIndex] || `Segment ${segmentIndex + 1}`,
        matches: rule.pairs.map(([leftIndex, rightIndex]) => {
          const left = slot.players[leftIndex];
          const right = slot.players[rightIndex];
          const start = segmentIndex === 0 ? 1 : 10;
          const end = segmentIndex === 0 ? 9 : 18;
          const leftSummary = summaries.get(normalize(left.name));
          const rightSummary = summaries.get(normalize(right.name));
          const holes = [];
          for (let holeNo = start; holeNo <= end; holeNo += 1) {
            const leftGross = leftSummary ? toIntOrNull(leftSummary.holes[holeNo - 1]) : null;
            const rightGross = rightSummary ? toIntOrNull(rightSummary.holes[holeNo - 1]) : null;
            holes.push({
              hole: holeNo,
              leftGross,
              rightGross,
              leftNet: leftGross === null ? null : leftGross - getHoleStrokeAllowance(left.hcp, holeNo, dayKey),
              rightNet: rightGross === null ? null : rightGross - getHoleStrokeAllowance(right.hcp, holeNo, dayKey),
            });
          }
          return {
            left: left.name,
            right: right.name,
            result: compareSegment(summaries, dayKey, left.name, right.name, segmentIndex),
            holes,
          };
        }),
      })),
    }));
    return acc;
  }, {});
}

function buildLeaderboard(state) {
  const day1Summaries = getDayPlayerSummaries(state, 'Day 1');
  const day3Summaries = getDayPlayerSummaries(state, 'Day 3');
  const day4Summaries = getDayPlayerSummaries(state, 'Day 4');
  const day1 = getMatchPointsFromLive(state, 'Day 1');
  const day2A = getMatchPointsFromLive(state, 'Day 2A');
  const day2B = getMatchPointsFromLive(state, 'Day 2B');
  const day3 = getMatchPointsFromLive(state, 'Day 3');
  const day4 = getMatchPointsFromLive(state, 'Day 4');
  const practice = getMatchPointsFromLive(state, 'Practice');
  const s1 = getStrokeBonusFromLive(state, 'Day 1');
  const s3 = getStrokeBonusFromLive(state, 'Day 3');
  const d4 = getDay4RankPointsFromLive(state);

  const totals = PLAYERS.map((player) => {
    const key = normalize(player.name);
    const penalty = getPenaltyEntry(state, player.name);
    const match1 = day1.points.get(key) || 0;
    const match2A = day2A.points.get(key) || 0;
    const match2B = day2B.points.get(key) || 0;
    const match2 = match2A + match2B;
    const match3 = day3.points.get(key) || 0;
    const stroke1 = s1.get(key) || 0;
    const stroke3 = s3.get(key) || 0;
    const day1Gross = toNumOrNull(day1Summaries.get(key) && day1Summaries.get(key).grossTotal);
    const day3Gross = toNumOrNull(day3Summaries.get(key) && day3Summaries.get(key).grossTotal);
    const day4Gross = toNumOrNull(day4Summaries.get(key) && day4Summaries.get(key).grossTotal);
    const day1Net = toNumOrNull(day1Summaries.get(key) && day1Summaries.get(key).adjustedNetTotal);
    const day3Net = toNumOrNull(day3Summaries.get(key) && day3Summaries.get(key).adjustedNetTotal);
    const day4Net = toNumOrNull(day4Summaries.get(key) && day4Summaries.get(key).adjustedNetTotal);
    const scramblePoints = getScramblePointsByPlayer(state);
    const scramble = scramblePoints.get(key) || 0;
    const day4Points = d4.pts.get(key) || 0;
    const day1Total = match1 + stroke1;
    const day2Total = match2;
    const day3Total = match3 + stroke3 + scramble;
    const total = Number((day1Total + day2Total + day3Total + day4Points).toFixed(2));
    return {
      name: player.name,
      match1,
      match2A,
      match2B,
      match2,
      match3,
      day1Gross,
      day3Gross,
      day4Gross,
      day1Net,
      day3Net,
      day4Net,
      stroke1,
      stroke3,
      scramble,
      penaltyChampion: penalty.champion,
      penaltyRookie: penalty.rookie,
      penaltyTotal: penalty.total,
      day4Points,
      day4Rank: d4.ranks.get(key),
      day1Total,
      day2Total,
      day3Total,
      total,
    };
  }).sort((a, b) => (b.total - a.total) || a.name.localeCompare(b.name));

  let pos = 0;
  let last = null;
  totals.forEach((row, idx) => {
    if (last === null || row.total !== last) pos = idx + 1;
    row.position = pos;
    last = row.total;
  });

  const leaderboard = {
    generatedAt: new Date().toISOString(),
    dayOptions: DAY_OPTIONS.slice(),
    matchDayOptions: MATCH_DAY_OPTIONS.slice(),
    matchBoards: {
      'Day 1': day1.rows,
      'Day 2A': day2A.rows,
      'Day 2B': day2B.rows,
      'Day 3': day3.rows,
      'Day 4': day4.rows,
      Practice: practice.rows,
    },
    matchDetails: buildMatchDetailBoards(state),
    totals,
    handicapSummary: buildHandicapSummary(state),
    scoreRankings: buildScoreRankings(state),
    skins: buildSkinsResults(state),
    scramble: getScrambleResults(state),
    sideGames: buildSideGameSummary(state),
    payouts: null,
    workbookResults: [],
  };
  leaderboard.workbookResults = buildWorkbookResultsAudit(state, leaderboard);
  return leaderboard;
}

function buildDayRows(leaderboard, selectedDay) {
  const source = Array.isArray(leaderboard && leaderboard.totals) ? leaderboard.totals : [];
  const rows = source.map((row) => {
    let points = 0;
    let detail = '';
    if (selectedDay === 'Day 1') {
      points = row.day1Total;
      detail = `${row.match1} match + ${row.stroke1} stroke`;
    } else if (selectedDay === 'Day 2A') {
      points = row.match2A;
      detail = `${row.match2A} match`;
    } else if (selectedDay === 'Day 2B') {
      points = row.match2B;
      detail = `${row.match2B} match`;
    } else if (selectedDay === 'Day 2 Total') {
      points = row.day2Total;
      detail = `${row.day2Total} match`;
    } else if (selectedDay === 'Day 3') {
      points = row.day3Total;
      detail = `${row.match3} match + ${row.stroke3} stroke + ${row.scramble} scramble`;
    } else {
      points = row.day4Points || 0;
      detail = `${row.day4Points} rank pts`;
    }
    if (Number(row.penaltyTotal) !== 0) {
      detail = `${detail} + pen ${row.penaltyTotal > 0 ? '+' : ''}${row.penaltyTotal}`;
    }
    return { name: row.name, points, detail, total: row.total };
  }).sort((a, b) => (b.points - a.points) || (b.total - a.total) || a.name.localeCompare(b.name));

  let pos = 0;
  let last = null;
  rows.forEach((row, idx) => {
    if (last === null || row.points !== last) pos = idx + 1;
    row.position = pos;
    last = row.points;
  });
  return rows;
}

function getLiveMeta(state) {
  const daySlots = MATCH_DAY_OPTIONS.map((dayKey) => ({
    dayKey,
    slots: getDaySlots(dayKey).map((slot) => ({
      slotIndex: slot.slotIndex,
      label: slot.label,
      players: slot.players.map((player) => player.name),
      hasCode: Boolean(state.codes[keyFor(dayKey, slot.slotIndex)]),
    })),
  }));
  return {
    dayOptions: DAY_OPTIONS.slice(),
    matchDayOptions: MATCH_DAY_OPTIONS.slice(),
    settings: state.settings || defaultTinCupLiveState().settings,
    penalties: buildPenaltyTable(state),
    playerHandicaps: PLAYERS.map((player) => ({ name: player.name, handicap: player.handicap })),
    daySlots,
    handicapSummary: buildHandicapSummary(state),
    scramble: getScrambleResults(state),
    sideGames: buildSideGameSummary(state),
    payouts: buildPayoutSummary(state),
    config: normalizeWorkbookConfig(state.config || defaultWorkbookConfig()),
  };
}

function getScorecardView(state, dayKey, slotIndex) {
  const { slot, scorecard } = ensureSlotScorecard(state, dayKey, slotIndex);
  const dayMarkers = getDayMarkers(state, dayKey);
  return {
    dayKey,
    slotIndex: Number(slotIndex),
    label: slot.label,
    players: slot.players.map((player) => {
      const entry = scorecard.players[normalize(player.name)] || { name: player.name, holes: holeArray() };
      const summary = summarizePlayerCard(entry, player.hcp, dayKey);
      const penalty = getPenaltyEntry(state, player.name);
      return {
        name: player.name,
        handicap: player.hcp,
        holes: summary.holes,
        grossTotal: summary.grossTotal,
        netTotal: summary.netTotal,
        penaltyChampion: penalty.champion,
        penaltyRookie: penalty.rookie,
        penaltyTotal: penalty.total,
        adjustedNetTotal: summary.netTotal === null ? null : Number((summary.netTotal + penalty.total).toFixed(2)),
        complete18: summary.complete18,
      };
    }),
    markers: dayMarkers,
    markerHoles: {
      ctp: getAllowedMarkerHoles('ctp'),
      longDrive: getAllowedMarkerHoles('longDrive'),
    },
    scorerName: scorecard.scorerName || '',
    submittedAt: scorecard.submittedAt || null,
    submittedBy: scorecard.submittedBy || '',
    submitted: Boolean(scorecard.submittedAt),
    updatedAt: scorecard.updatedAt || null,
  };
}

function updateHoleScore(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const slotIndex = Number(payload.slotIndex);
  const playerName = clean(payload.playerName);
  const hole = Number(payload.hole);
  const gross = toIntOrNull(payload.gross);
  if (!dayKey) throw new Error('dayKey required');
  if (!Number.isInteger(slotIndex) || slotIndex < 0) throw new Error('slotIndex required');
  if (!playerName) throw new Error('playerName required');
  if (!Number.isInteger(hole) || hole < 1 || hole > 18) throw new Error('hole must be 1-18');

  const { slot, scorecard } = ensureSlotScorecard(state, dayKey, slotIndex);
  assertScorecardEditable(scorecard, payload.allowSubmittedEdit === true);
  const player = slot.players.find((entry) => normalize(entry.name) === normalize(playerName));
  if (!player) throw new Error('Player not in this foursome');
  const scorerName = clean(payload.scorerName);
  if (scorerName) scorecard.scorerName = scorerName;

  const key = normalize(player.name);
  const entry = scorecard.players[key] || { name: player.name, holes: holeArray() };
  while (entry.holes.length < 18) entry.holes.push(null);
  entry.holes[hole - 1] = gross;
  scorecard.players[key] = entry;
  scorecard.updatedAt = new Date().toISOString();
  return getScorecardView(state, dayKey, slotIndex);
}

function updateMarker(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const slotIndex = Number(payload.slotIndex);
  const type = clean(payload.type);
  const hole = Number(payload.hole);
  const winner = clean(payload.winner);
  if (!dayKey) throw new Error('dayKey required');
  if (!Number.isInteger(slotIndex) || slotIndex < 0) throw new Error('slotIndex required');
  if (!['ctp', 'longDrive'].includes(type)) throw new Error('Marker type must be ctp or longDrive');
  if (!Number.isInteger(hole) || hole < 1 || hole > 18) throw new Error('hole must be 1-18');
  if (type === 'ctp' && !getAllowedMarkerHoles('ctp').includes(hole)) throw new Error('CTP is only allowed on par-3 holes');

  const { slot, scorecard } = ensureSlotScorecard(state, dayKey, slotIndex);
  assertScorecardEditable(scorecard, payload.allowSubmittedEdit === true);
  const allowed = new Map(slot.players.map((player) => [normalize(player.name), player.name]));
  const scorerName = clean(payload.scorerName);
  if (scorerName) scorecard.scorerName = scorerName;
  clearMarkerAssignments(state, dayKey, type, hole);
  if (winner) {
    const winnerKey = normalize(winner);
    if (!allowed.has(winnerKey)) throw new Error('Winner must be in this foursome');
    scorecard.markers[type][String(hole)] = allowed.get(winnerKey);
  }
  scorecard.updatedAt = new Date().toISOString();
  return getScorecardView(state, dayKey, slotIndex);
}

function setScorecardScorer(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const slotIndex = Number(payload.slotIndex);
  const scorerName = clean(payload.scorerName);
  if (!dayKey) throw new Error('dayKey required');
  if (!Number.isInteger(slotIndex) || slotIndex < 0) throw new Error('slotIndex required');
  if (!scorerName) throw new Error('scorerName required');
  const { scorecard } = ensureSlotScorecard(state, dayKey, slotIndex);
  scorecard.scorerName = scorerName;
  scorecard.updatedAt = new Date().toISOString();
  return getScorecardView(state, dayKey, slotIndex);
}

function submitScorecard(state, payload = {}) {
  const dayKey = clean(payload.dayKey);
  const slotIndex = Number(payload.slotIndex);
  const scorerName = clean(payload.scorerName);
  if (!dayKey) throw new Error('dayKey required');
  if (!Number.isInteger(slotIndex) || slotIndex < 0) throw new Error('slotIndex required');

  const { slot, scorecard } = ensureSlotScorecard(state, dayKey, slotIndex);
  if (scorerName) scorecard.scorerName = scorerName;
  const incompletePlayer = slot.players.find((player) => {
    const entry = scorecard.players[normalize(player.name)] || { name: player.name, holes: holeArray() };
    const summary = summarizePlayerCard(entry, player.hcp, dayKey);
    return summary.complete18 !== true;
  });
  if (incompletePlayer) {
    throw new Error('All 18 holes for all players must be entered before submitting the scorecard.');
  }

  if (!scorecard.submittedAt) scorecard.submittedAt = new Date().toISOString();
  if (scorerName) scorecard.submittedBy = scorerName;
  if (!scorecard.submittedBy) scorecard.submittedBy = 'Unknown scorer';
  scorecard.updatedAt = new Date().toISOString();
  return getScorecardView(state, dayKey, slotIndex);
}

function setScrambleBonus(state, playerName, value) {
  const key = normalize(playerName);
  if (!key) throw new Error('playerName required');
  const parsed = toNumOrNull(value);
  if (parsed === null) delete state.scrambleBonus[key];
  else state.scrambleBonus[key] = parsed;
  return state.scrambleBonus;
}

function setPlayerPenalty(state, playerName, payload = {}) {
  const key = normalize(playerName);
  if (!key) throw new Error('playerName required');
  if (!PLAYER_KEYS.has(key)) throw new Error('Unknown Tin Cup player');
  if (!state.penalties || typeof state.penalties !== 'object') state.penalties = {};
  const current = getPenaltyEntry(state, playerName);
  const champion = Object.prototype.hasOwnProperty.call(payload, 'champion')
    ? toPenalty(payload.champion)
    : current.champion;
  const rookie = Object.prototype.hasOwnProperty.call(payload, 'rookie')
    ? toPenalty(payload.rookie)
    : current.rookie;
  if (!champion && !rookie) {
    delete state.penalties[key];
  } else {
    state.penalties[key] = { champion, rookie };
  }
  return buildPenaltyTable(state);
}

function getSeedGrossScoreWithRandomizer(dayKey, slotIndex, player, holeNumber, randomizer = null) {
  const playerKey = normalize(player && player.name);
  const playerIndex = Math.max(0, PLAYERS.findIndex((entry) => normalize(entry.name) === playerKey));
  const dayIndex = Math.max(0, MATCH_DAY_OPTIONS.indexOf(dayKey));
  const skillOffset = Math.max(0, Math.min(3, Math.round((Number(player && player.hcp) - 4) / 4)));
  let gross = 4 + skillOffset;
  if (((holeNumber + playerIndex + dayIndex + Number(slotIndex)) % 6) === 0) gross -= 1;
  if (((holeNumber * (Number(slotIndex) + 2) + playerIndex + dayIndex) % 9) === 0) gross += 1;
  if (((holeNumber + dayIndex + playerIndex) % 13) === 0) gross += 1;
  if (((holeNumber + Number(slotIndex) + playerIndex) % 11) === 0) gross -= 1;
  if (randomizer) {
    gross += randomizer.getPlayerTripBias(playerKey);
    gross += randomizer.getPlayerDayBias(playerKey, dayKey);
    gross += randomizer.getPlayerHoleBias(playerKey, dayKey, holeNumber);
    const roll = randomizer.int(100);
    if (roll < 18) gross -= 1;
    else if (roll > 80) gross += 1;
    if (randomizer.int(100) < 8) gross += 1;
    if (randomizer.int(100) < 8) gross -= 1;
  }
  return Math.max(3, Math.min(8, gross));
}

function createSeedRandomizer() {
  const tripBias = new Map();
  const dayBias = new Map();
  const holeBias = new Map();
  return {
    int(max) {
      return crypto.randomInt(Math.max(1, Number(max) || 1));
    },
    pick(list = []) {
      if (!Array.isArray(list) || !list.length) return null;
      return list[this.int(list.length)];
    },
    getPlayerTripBias(playerKey = '') {
      const key = String(playerKey || '');
      if (!tripBias.has(key)) tripBias.set(key, this.int(5) - 2);
      return tripBias.get(key) || 0;
    },
    getPlayerDayBias(playerKey = '', dayKey = '') {
      const key = `${playerKey}|${dayKey}`;
      if (!dayBias.has(key)) dayBias.set(key, this.int(5) - 2);
      return dayBias.get(key) || 0;
    },
    getPlayerHoleBias(playerKey = '', dayKey = '', holeNumber = 0) {
      const key = `${playerKey}|${dayKey}|${holeNumber}`;
      if (!holeBias.has(key)) {
        const roll = this.int(100);
        holeBias.set(key, roll < 8 ? -1 : roll > 91 ? 1 : 0);
      }
      return holeBias.get(key) || 0;
    },
  };
}

function getSeedMarkerWinnerFromScorecard(scorecard, players = [], holeNumber, randomizer = null) {
  if (!scorecard || !scorecard.players || !Array.isArray(players) || !players.length) return '';
  let best = null;
  players.forEach((player, playerIndex) => {
    const entry = scorecard.players[normalize(player.name)];
    const gross = toIntOrNull(entry && Array.isArray(entry.holes) ? entry.holes[holeNumber - 1] : null);
    if (gross === null) return;
    const tieBreaker = randomizer ? randomizer.int(1000) : playerIndex;
    if (!best || gross < best.gross || (gross === best.gross && tieBreaker < best.tieBreaker)) {
      best = { name: player.name, gross, tieBreaker };
    }
  });
  return best ? best.name : '';
}

function ensureSeedSnowmanCandidate(state, dayKey, randomizer) {
  const slots = getDaySlots(dayKey).filter((slot) => Array.isArray(slot.players) && slot.players.length);
  if (!slots.length) return;
  const slot = randomizer.pick(slots);
  if (!slot) return;
  const player = randomizer.pick(slot.players);
  if (!player) return;
  const scorecard = getStoredScorecard(state, dayKey, slot.slotIndex);
  if (!scorecard || !scorecard.players) return;
  const entry = scorecard.players[normalize(player.name)];
  if (!entry || !Array.isArray(entry.holes) || !entry.holes.length) return;
  const holeIndex = randomizer.int(Math.min(18, entry.holes.length));
  entry.holes[holeIndex] = 8;
}

function seedAllScores(state, options = {}) {
  const reset = options.reset !== false;
  const randomizer = createSeedRandomizer();
  if (reset) {
    clearCompetitionState(state, {
      preserveCodes: true,
      preservePenalties: false,
      preserveConfig: true,
      preserveSettings: true,
    });
  }
  if (!state.scorecards || typeof state.scorecards !== 'object') state.scorecards = {};
  if (!state.scrambleBonus || typeof state.scrambleBonus !== 'object') state.scrambleBonus = {};
  if (!state.scramble || typeof state.scramble !== 'object') state.scramble = normalizeScrambleState();
  if (!state.sideGames || typeof state.sideGames !== 'object') state.sideGames = normalizeSideGamesState();
  if (!state.penalties || typeof state.penalties !== 'object') state.penalties = {};

  state.penalties = {
    ...(reset ? {} : state.penalties),
    ...normalizePenalties(SEED_PLAYER_PENALTIES),
  };
  state.scrambleBonus = {
    ...(reset ? {} : state.scrambleBonus),
    ...SEED_SCRAMBLE_BONUS,
  };
  state.sideGames = normalizeSideGamesState();
  SIDE_GAME_DEFS.longPutt.days.forEach((dayKey) => {
    const winner = randomizer.pick(PLAYERS);
    if (winner) updateSideGameWinner(state, { type: 'longPutt', dayKey, winner: winner.name });
  });

  MATCH_DAY_OPTIONS.forEach((dayKey) => {
    getDaySlots(dayKey).forEach((slot) => {
      const { scorecard } = ensureSlotScorecard(state, dayKey, slot.slotIndex);
      scorecard.players = {};
      scorecard.scorerName = '';
      slot.players.forEach((player) => {
        scorecard.players[normalize(player.name)] = {
          name: player.name,
          holes: Array.from({ length: 18 }, (_, index) => getSeedGrossScoreWithRandomizer(dayKey, slot.slotIndex, player, index + 1, randomizer)),
        };
      });
      scorecard.markers = { ctp: {}, longDrive: {} };
      scorecard.submittedAt = null;
      scorecard.submittedBy = '';
      SEED_MARKER_HOLES.ctp.forEach((holeNumber) => {
        scorecard.markers.ctp[String(holeNumber)] = getSeedMarkerWinnerFromScorecard(scorecard, slot.players, holeNumber, randomizer);
      });
      SEED_MARKER_HOLES.longDrive.forEach((holeNumber) => {
        scorecard.markers.longDrive[String(holeNumber)] = getSeedMarkerWinnerFromScorecard(scorecard, slot.players, holeNumber, randomizer);
      });
      scorecard.updatedAt = new Date().toISOString();
      state.scorecards[keyFor(dayKey, slot.slotIndex)] = scorecard;
    });
  });

  getScrambleTeams().forEach((team) => {
    const { holes } = ensureScrambleTeamScores(state, team.teamIndex);
    holes.splice(0, holes.length, ...ALL_HOLES.map((holeNumber) => {
      const seedPlayers = team.players.map((name) => ({ name, hcp: getDayHandicapMap('Day 1').get(normalize(name)) || 0 }));
      return Math.min(...seedPlayers.map((player) => getSeedGrossScoreWithRandomizer('Day 3', team.teamIndex, player, holeNumber, randomizer)));
    }));
  });

  SIDE_GAME_DEFS.secretSnowman.days.forEach((dayKey) => {
    ensureSeedSnowmanCandidate(state, dayKey, randomizer);
    state.sideGames.secretSnowman[dayKey] = '';
    maybeAutoDrawSecretSnowman(state, { dayKey });
  });

  const leaderboard = buildLeaderboard(state);
  leaderboard.payouts = buildPayoutSummary(state, leaderboard);
  return leaderboard;
}

module.exports = {
  DAY_OPTIONS,
  MATCH_DAY_OPTIONS,
  PLAYERS,
  defaultTinCupLiveState,
  ensureTinCupLiveState,
  getLiveMeta,
  updateSettings,
  clearCompetitionState,
  setSlotCode,
  verifySlotCode,
  getScorecardView,
  updateHoleScore,
  updateMarker,
  setScorecardScorer,
  submitScorecard,
  buildLeaderboard,
  buildDayRows,
  setScrambleBonus,
  updateScrambleHoleScore,
  setPlayerPenalty,
  seedAllScores,
  buildHandicapSummary,
  buildScoreRankings,
  buildSkinsResults,
  getScrambleResults,
  buildSideGameSummary,
  buildPayoutSummary,
  buildSeedSummary,
  buildWorkbookResultsAudit,
  buildCompetitionExportRows,
  buildCompetitionExportCsv,
  updateWorkbookConfig,
  updateSideGameWinner,
  pickSecretSnowmanWinner,
  maybeAutoSubmitScorecard,
  maybeAutoDrawSecretSnowman,
};

