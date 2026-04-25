const express = require('express');
const { getSecondaryConn, initSecondaryConn } = require('../secondary-conn');
initSecondaryConn();
const ADMIN_DELETE_CODE = process.env.ADMIN_DELETE_CODE || '';
const SITE_ADMIN_WRITE_CODE = process.env.SITE_ADMIN_WRITE_CODE || '2000';
const ADMIN_DESTRUCTIVE_CODE = process.env.ADMIN_DESTRUCTIVE_CODE || ADMIN_DELETE_CODE;
const ADMIN_DESTRUCTIVE_CONFIRM_CODE = process.env.ADMIN_DESTRUCTIVE_CONFIRM_CODE || '';
const TripPrimary = require('../models/Trip');
const TripParticipantPrimary = require('../models/TripParticipant');
const TripAuditLogPrimary = require('../models/TripAuditLog');
const {
  buildRyderCupTripTemplate,
  RYDER_CUP_TEMPLATE_NAME,
} = require('../services/tripTemplateService');
const {
  buildTripCompetitionView,
  setRoundMatchTeams,
  setRoundPlayerScores,
  setRoundSideGames,
  setTripRyderCupRound,
  setTripRyderCupSettings,
  setTripRyderCupTeams,
  syncTripRyderCupOverlayToCompetition,
  swapTripRyderCupTeamPlayers,
  setTripHandicapBuckets,
  setTripScoringMode,
  normalizeLegacyMyrtleTripTeeSheet,
} = require('../services/tripCompetitionService');
const {
  ensureTripRyderCupState,
  setTripRyderCupState,
} = require('../services/tripRyderCupService');
const {
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
  updateSideGameWinner,
  pickSecretSnowmanWinner,
  setPlayerPenalty,
  seedAllScores,
  buildPayoutSummary,
  buildSeedSummary,
  buildCompetitionExportCsv,
  updateWorkbookConfig,
  maybeAutoSubmitScorecard,
  maybeAutoDrawSecretSnowman,
} = require('../services/tinCupLiveService');
const router = express.Router();

function getSecondaryModels() {
  const conn = getSecondaryConn();
  if (!conn) return {};
  return {
    TripSecondary: conn.model('Trip', require('../models/Trip').schema),
    TripParticipantSecondary: conn.model('TripParticipant', require('../models/TripParticipant').schema),
    TripAuditLogSecondary: conn.model('TripAuditLog', require('../models/TripAuditLog').schema),
  };
}

function isAdmin(req) {
  const code = req.headers['x-admin-code'] || req.query.code || (req.body && (req.body.code || req.body.adminCode));
  return Boolean(SITE_ADMIN_WRITE_CODE && code && code === SITE_ADMIN_WRITE_CODE);
}

function isDeleteAdmin(req) {
  const code = req.headers['x-admin-delete-code']
    || req.query.deleteCode
    || (req.body && req.body.deleteCode)
    || req.headers['x-admin-code']
    || req.query.code
    || (req.body && (req.body.code || req.body.adminCode));
  return Boolean(ADMIN_DESTRUCTIVE_CODE && code && code === ADMIN_DESTRUCTIVE_CODE);
}

function hasDestructiveConfirm(req) {
  if (!ADMIN_DESTRUCTIVE_CONFIRM_CODE) return true;
  const code = req.headers['x-admin-confirm-code']
    || req.query.confirmCode
    || (req.body && req.body.confirmCode)
    || '';
  return code === ADMIN_DESTRUCTIVE_CONFIRM_CODE;
}

function getTripModelsForRequest(req) {
  if (req.query.myrtleBeach2026 === 'true') {
    const { TripSecondary, TripParticipantSecondary, TripAuditLogSecondary } = getSecondaryModels();
    if (TripSecondary && TripParticipantSecondary && TripAuditLogSecondary) {
      return {
        TripModel: TripSecondary,
        TripParticipantModel: TripParticipantSecondary,
        TripAuditLogModel: TripAuditLogSecondary,
      };
    }
  }
  return {
    TripModel: TripPrimary,
    TripParticipantModel: TripParticipantPrimary,
    TripAuditLogModel: TripAuditLogPrimary,
  };
}

async function loadTripBundle(req) {
  const { TripModel, TripParticipantModel, TripAuditLogModel } = getTripModelsForRequest(req);
  const trip = await TripModel.findById(req.params.tripId);
  if (!trip) return { TripModel, TripParticipantModel, TripAuditLogModel, trip: null, participants: [] };
  const participants = await TripParticipantModel.find({ trip: trip._id });
  return { TripModel, TripParticipantModel, TripAuditLogModel, trip, participants };
}

function sendTripRouteError(res, error) {
  const message = error && error.message ? error.message : 'Request failed';
  if (error && Number.isInteger(error.status)) return res.status(error.status).json({ error: message });
  if (/not found/i.test(message)) return res.status(404).json({ error: message });
  if (/required|select exactly|four-player|groups of 4|even player count|unique players|seed rank/i.test(message)) return res.status(400).json({ error: message });
  return res.status(500).json({ error: message });
}

function isLiveScoringEnabled(state) {
  return Boolean(state && state.settings && state.settings.enableLiveFoursomeScoring);
}

function parseRoundStartDate(round) {
  if (!round || !round.date) return null;
  const day = new Date(round.date);
  if (Number.isNaN(day.getTime())) return null;
  const hhmm = String(round.time || '').trim();
  const match = /^(\d{1,2}):(\d{2})$/.exec(hhmm);
  if (!match) {
    day.setHours(0, 0, 0, 0);
    return day;
  }
  day.setHours(Number(match[1]), Number(match[2]), 0, 0);
  return day;
}

function getTripStartDate(trip) {
  if (!trip) return null;
  let earliest = null;
  const rounds = Array.isArray(trip.rounds) ? trip.rounds : [];
  rounds.forEach((round) => {
    const dt = parseRoundStartDate(round);
    if (!dt) return;
    if (!earliest || dt.getTime() < earliest.getTime()) earliest = dt;
  });
  if (earliest) return earliest;
  if (trip.arrivalDate) {
    const arrival = new Date(trip.arrivalDate);
    if (!Number.isNaN(arrival.getTime())) {
      arrival.setHours(0, 0, 0, 0);
      return arrival;
    }
  }
  return null;
}

function getTripEndDate(trip) {
  if (!trip) return null;
  let latest = null;
  const rounds = Array.isArray(trip.rounds) ? trip.rounds : [];
  rounds.forEach((round) => {
    const dt = parseRoundStartDate(round);
    if (!dt) return;
    if (!latest || dt.getTime() > latest.getTime()) latest = dt;
  });
  if (trip.departureDate) {
    const departure = new Date(trip.departureDate);
    if (!Number.isNaN(departure.getTime())) {
      departure.setHours(23, 59, 59, 999);
      if (!latest || departure.getTime() > latest.getTime()) latest = departure;
    }
  }
  return latest;
}

function hasTripStarted(trip) {
  const start = getTripStartDate(trip);
  if (!start) return false;
  return Date.now() >= start.getTime();
}

function hasTripEnded(trip) {
  const end = getTripEndDate(trip);
  if (!end) return false;
  return Date.now() > end.getTime();
}

function formatTripDateLabel(value) {
  if (!value) return '';
  const dt = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(dt.getTime())) return '';
  return dt.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}

function sanitizeCsvFilename(value = '') {
  const safe = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return safe || 'tin-cup-competition';
}

function truncateValue(value, maxLen = 240) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return value.length > maxLen ? `${value.slice(0, maxLen)}...` : value;
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.slice(0, 20).map((item) => truncateValue(item, maxLen));
  if (typeof value === 'object') {
    const out = {};
    Object.keys(value).slice(0, 40).forEach((key) => {
      out[key] = truncateValue(value[key], maxLen);
    });
    return out;
  }
  return String(value);
}

async function writeTripAudit(req, trip, TripAuditLogModel, action, summary, details = {}) {
  if (!trip || !TripAuditLogModel) return;
  if (!hasTripStarted(trip)) return;
  try {
    await TripAuditLogModel.create({
      tripId: trip._id,
      action: String(action || '').trim() || 'trip_update',
      actor: isAdmin(req) ? 'admin' : 'public',
      method: String(req.method || ''),
      route: String(req.originalUrl || req.path || ''),
      summary: String(summary || '').trim(),
      details: truncateValue(details),
      timestamp: new Date(),
    });
  } catch (_error) {
    // Audit logging should never break trip updates.
  }
}


// List all trips
router.get('/', async (req, res) => {
  // If query param myrtleBeach2026=true, use secondary DB
  if (req.query.myrtleBeach2026 === 'true') {
    const { TripSecondary } = getSecondaryModels();
    if (TripSecondary) {
      const trips = await TripSecondary.find();
      return res.json(trips.map((trip) => normalizeLegacyMyrtleTripTeeSheet(trip)));
    }
  }
  const trips = await TripPrimary.find();
  res.json(trips.map((trip) => normalizeLegacyMyrtleTripTeeSheet(trip)));
});

router.post('/templates/ryder-cup', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { TripModel, TripParticipantModel } = getTripModelsForRequest(req);
    const payload = buildRyderCupTripTemplate(req.body || {});
    const trip = await TripModel.create(payload.trip);
    const participants = await TripParticipantModel.insertMany(
      (payload.participants || []).map((participant) => ({
        ...participant,
        trip: trip._id,
      }))
    );
    if (payload.ryderCup) {
      const state = setTripRyderCupState(trip, participants, payload.ryderCup);
      syncTripRyderCupOverlayToCompetition(trip, state);
      await trip.save();
    }
    return res.status(201).json({
      trip: normalizeLegacyMyrtleTripTeeSheet(trip),
      participants,
      templateName: RYDER_CUP_TEMPLATE_NAME,
      message: 'Ryder Cup trip created from template.',
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

// Get trip details + participants
router.get('/:tripId', async (req, res) => {
  // If query param myrtleBeach2026=true, use secondary DB
  if (req.query.myrtleBeach2026 === 'true') {
    const { TripSecondary, TripParticipantSecondary } = getSecondaryModels();
    if (TripSecondary && TripParticipantSecondary) {
      const trip = await TripSecondary.findById(req.params.tripId);
      if (!trip) return res.status(404).json({ error: 'Trip not found' });
      const participants = await TripParticipantSecondary.find({ trip: trip._id });
      return res.json({ trip: normalizeLegacyMyrtleTripTeeSheet(trip), participants });
    }
  }
  const trip = await TripPrimary.findById(req.params.tripId);
  if (!trip) return res.status(404).json({ error: 'Trip not found' });
  const participants = await TripParticipantPrimary.find({ trip: trip._id });
  res.json({ trip: normalizeLegacyMyrtleTripTeeSheet(trip), participants });
});

router.get('/:tripId/rydercup', async (req, res) => {
  try {
    const { trip, participants } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const { state, changed } = ensureTripRyderCupState(trip, participants);
    if (changed) await trip.save();
    return res.json(state);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

// Update trip details
router.put('/:tripId', async (req, res) => {
  try {
    const { TripModel, TripAuditLogModel } = getTripModelsForRequest(req);
    const trip = await TripModel.findByIdAndUpdate(req.params.tripId, req.body, { new: true });
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    await writeTripAudit(req, trip, TripAuditLogModel, 'update_trip', 'Trip details updated', {
      updates: req.body || {},
    });
    return res.json(normalizeLegacyMyrtleTripTeeSheet(trip));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/rydercup', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const currentState = trip && trip.ryderCup && typeof trip.ryderCup.toObject === 'function'
      ? trip.ryderCup.toObject()
      : (trip.ryderCup || {});
    const payload = {
      ...currentState,
      ...(req.body || {}),
    };
    const state = setTripRyderCupState(trip, participants, payload);
    syncTripRyderCupOverlayToCompetition(trip, state);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'trip_ryder_cup', 'Trip Ryder Cup rosters updated', {
      enabled: state.enabled,
      teamAName: state.teamAName,
      teamBName: state.teamBName,
      teamACount: Array.isArray(state.teamAPlayers) ? state.teamAPlayers.length : 0,
      teamBCount: Array.isArray(state.teamBPlayers) ? state.teamBPlayers.length : 0,
    });
    return res.json(state);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/competition', async (req, res) => {
  try {
    const { trip, participants } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    return res.json(buildTripCompetitionView(normalizeLegacyMyrtleTripTeeSheet(trip), participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/settings', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setTripScoringMode(trip, req.body && req.body.scoringMode);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'competition_settings', 'Competition scoring mode changed', {
      scoringMode: req.body && req.body.scoringMode,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/buckets', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setTripHandicapBuckets(trip, participants, req.body && req.body.buckets);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'competition_buckets', 'Competition handicap buckets updated', {
      bucketCount: Array.isArray(req.body && req.body.buckets) ? req.body.buckets.length : 0,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/rounds/:roundIndex/scores', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setRoundPlayerScores(trip, req.params.roundIndex, req.body && req.body.playerName, req.body && req.body.holes);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'round_scores', 'Round player scores updated', {
      roundIndex: Number(req.params.roundIndex),
      playerName: req.body && req.body.playerName,
      holeCount: Array.isArray(req.body && req.body.holes) ? req.body.holes.length : 0,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/rounds/:roundIndex/matches/:slotIndex', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setRoundMatchTeams(trip, req.params.roundIndex, req.params.slotIndex, req.body && req.body.teamA, req.body && req.body.teamB);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'round_matches', 'Round match teams updated', {
      roundIndex: Number(req.params.roundIndex),
      slotIndex: Number(req.params.slotIndex),
      teamA: req.body && req.body.teamA,
      teamB: req.body && req.body.teamB,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/rounds/:roundIndex/side-games', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setRoundSideGames(trip, req.params.roundIndex, {
      ctpWinners: req.body && req.body.ctpWinners,
      skinsResults: req.body && req.body.skinsResults,
    });
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'round_side_games', 'Round side games updated', {
      roundIndex: Number(req.params.roundIndex),
      ctpCount: Array.isArray(req.body && req.body.ctpWinners) ? req.body.ctpWinners.length : 0,
      skinsCount: Array.isArray(req.body && req.body.skinsResults) ? req.body.skinsResults.length : 0,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/ryder-cup/teams', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setTripRyderCupTeams(trip, {
      teams: req.body && req.body.teams,
    });
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'ryder_cup_teams', 'Ryder Cup teams updated', {
      teams: req.body && req.body.teams,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/ryder-cup/teams/swap', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    swapTripRyderCupTeamPlayers(trip, req.body && req.body.playerName, req.body && req.body.targetPlayerName);
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'ryder_cup_team_swap', 'Ryder Cup players swapped between teams', {
      playerName: req.body && req.body.playerName,
      targetPlayerName: req.body && req.body.targetPlayerName,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/ryder-cup/rounds/:roundIndex', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setTripRyderCupRound(trip, req.params.roundIndex, req.body || {});
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'ryder_cup_round', 'Ryder Cup round updated', {
      roundIndex: Number(req.params.roundIndex),
      matchCount: Array.isArray(req.body && req.body.matches) ? req.body.matches.length : 0,
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/competition/ryder-cup/settings', async (req, res) => {
  if (!isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { trip, participants, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    setTripRyderCupSettings(trip, req.body || {});
    await trip.save();
    await writeTripAudit(req, trip, TripAuditLogModel, 'ryder_cup_settings', 'Ryder Cup settings updated', {
      hasSideGames: Boolean(req.body && req.body.sideGames),
      hasPayout: Boolean(req.body && req.body.payout),
    });
    return res.json(buildTripCompetitionView(trip, participants));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/meta', async (req, res) => {
  try {
    const { trip } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    return res.json(getLiveMeta(state));
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/leaderboard', async (req, res) => {
  try {
    const { trip } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    const selectedDay = String(req.query.day || '').trim();
    const selectedMatchDay = String(req.query.matchDay || '').trim();
    const leaderboard = buildLeaderboard(state);
    const dayKey = leaderboard.dayOptions.includes(selectedDay) ? selectedDay : leaderboard.dayOptions[0];
    const matchKey = leaderboard.matchDayOptions.includes(selectedMatchDay) ? selectedMatchDay : leaderboard.matchDayOptions[0];
    leaderboard.payouts = buildPayoutSummary(state, leaderboard);
    return res.json({
      ...leaderboard,
      settings: state.settings,
      selectedDay: dayKey,
      selectedMatchDay: matchKey,
      dayRows: buildDayRows(leaderboard, dayKey),
      matchRows: leaderboard.matchBoards[matchKey] || [],
      matchDetailRows: leaderboard.matchDetails[matchKey] || [],
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/export.csv', async (req, res) => {
  try {
    const { trip, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
    const tripEnd = getTripEndDate(trip);
    if (!tripEnd) return res.status(400).json({ error: 'Trip end date unavailable for final CSV export.' });
    if (!hasTripEnded(trip)) {
      return res.status(403).json({ error: `Final CSV export is available after ${formatTripDateLabel(tripEnd)}.` });
    }
    const state = ensureTinCupLiveState(trip);
    const csv = buildCompetitionExportCsv(state, {
      tripId: String(trip._id || ''),
      tripName: String(trip.name || trip.groupName || 'Tin Cup 2026'),
      tripStartDate: getTripStartDate(trip),
      tripEndDate: tripEnd,
      exportedAt: new Date(),
    });
    const filename = `${sanitizeCsvFilename(trip.name || trip.groupName || 'tin-cup-competition')}-final-competition.csv`;
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_export_csv', 'Exported final Tin Cup competition CSV', {
      filename,
      rowCount: csv ? Math.max(csv.split('\n').length - 1, 0) : 0,
    });
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Cache-Control', 'no-store');
    return res.send(`\ufeff${csv}`);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/scorecard', async (req, res) => {
  try {
    const { trip, TripModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const dayKey = String(req.query.dayKey || '').trim();
    const slotIndex = Number(req.query.slotIndex);
    const code = String(req.query.code || '').trim();
    if (!dayKey) return res.status(400).json({ error: 'dayKey required' });
    if (!Number.isInteger(slotIndex) || slotIndex < 0) return res.status(400).json({ error: 'slotIndex required' });
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    if (state.settings.enableFoursomeCodes && !code) return res.status(403).json({ error: 'Foursome code required' });
    if (!verifySlotCode(state, dayKey, slotIndex, code)) return res.status(403).json({ error: 'Invalid foursome code' });
    const autoSubmit = maybeAutoSubmitScorecard(state, { dayKey, slotIndex });
    const view = autoSubmit && autoSubmit.view ? autoSubmit.view : getScorecardView(state, dayKey, slotIndex);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    return res.json({
      ...view,
      autoSubmitted: Boolean(autoSubmit && autoSubmit.autoSubmitted),
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/scorecard/open', async (req, res) => {
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const dayKey = String(payload.dayKey || '').trim();
    const slotIndex = Number(payload.slotIndex);
    const code = String(payload.code || '').trim();
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    if (state.settings.enableFoursomeCodes && !code) return res.status(403).json({ error: 'Foursome code required' });
    if (!verifySlotCode(state, dayKey, slotIndex, code)) return res.status(403).json({ error: 'Invalid foursome code' });
    let view = setScorecardScorer(state, payload);
    const autoSubmit = maybeAutoSubmitScorecard(state, payload);
    if (autoSubmit && autoSubmit.view) view = autoSubmit.view;
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scorecard_open', 'Tin Cup live scorecard opened', {
      dayKey,
      slotIndex,
      scorerName: payload.scorerName,
    });
    if (autoSubmit && autoSubmit.autoSubmitted) {
      await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scorecard_auto_submit', 'Tin Cup live scorecard auto-submitted after completion', {
        dayKey,
        slotIndex,
        scorerName: payload.scorerName,
      });
    }
    return res.json({
      ...view,
      autoSubmitted: Boolean(autoSubmit && autoSubmit.autoSubmitted),
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/scorecard/hole', async (req, res) => {
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const dayKey = String(payload.dayKey || '').trim();
    const slotIndex = Number(payload.slotIndex);
    const code = String(payload.code || '').trim();
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    if (state.settings.enableFoursomeCodes && !code) return res.status(403).json({ error: 'Foursome code required' });
    if (!verifySlotCode(state, dayKey, slotIndex, code)) return res.status(403).json({ error: 'Invalid foursome code' });
    let view = updateHoleScore(state, { ...payload, allowSubmittedEdit: isAdmin(req) });
    const autoSubmit = maybeAutoSubmitScorecard(state, payload);
    if (autoSubmit && autoSubmit.view) view = autoSubmit.view;
    const autoSnowman = maybeAutoDrawSecretSnowman(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_hole_score', 'Tin Cup live hole score updated', {
      dayKey,
      slotIndex,
      hole: payload.hole,
      playerName: payload.playerName,
      gross: payload.gross,
      scorerName: payload.scorerName,
    });
    if (autoSubmit && autoSubmit.autoSubmitted) {
      await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scorecard_auto_submit', 'Tin Cup live scorecard auto-submitted after completion', {
        dayKey,
        slotIndex,
        scorerName: payload.scorerName,
      });
    }
    if (autoSnowman && autoSnowman.autoDrawn && autoSnowman.picked) {
      await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_secret_snowman_auto_draw', 'Tin Cup Secret Snowman auto-drawn after day completion', {
        dayKey,
        winner: autoSnowman.picked.playerName,
        slotIndex: autoSnowman.picked.slotIndex,
        slotLabel: autoSnowman.picked.label,
        hole: autoSnowman.picked.hole,
        gross: autoSnowman.picked.gross,
      });
    }
    return res.json({
      ...view,
      autoSubmitted: Boolean(autoSubmit && autoSubmit.autoSubmitted),
      autoSecretSnowman: autoSnowman && autoSnowman.autoDrawn ? autoSnowman.picked : null,
      sideGames: autoSnowman && autoSnowman.autoDrawn ? autoSnowman.sideGames : undefined,
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/scorecard/marker', async (req, res) => {
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const dayKey = String(payload.dayKey || '').trim();
    const slotIndex = Number(payload.slotIndex);
    const code = String(payload.code || '').trim();
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    if (!state.settings.enableLiveMarkers) return res.status(403).json({ error: 'Live marker entry is disabled for this trip.' });
    if (state.settings.enableFoursomeCodes && !code) return res.status(403).json({ error: 'Foursome code required' });
    if (!verifySlotCode(state, dayKey, slotIndex, code)) return res.status(403).json({ error: 'Invalid foursome code' });
    const view = updateMarker(state, { ...payload, allowSubmittedEdit: isAdmin(req) });
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_marker', 'Tin Cup live marker updated', {
      dayKey,
      slotIndex,
      markerType: payload.type,
      hole: payload.hole,
      winner: payload.winner,
      scorerName: payload.scorerName,
    });
    return res.json(view);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/scorecard/submit', async (req, res) => {
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const dayKey = String(payload.dayKey || '').trim();
    const slotIndex = Number(payload.slotIndex);
    const code = String(payload.code || '').trim();
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    if (state.settings.enableFoursomeCodes && !code) return res.status(403).json({ error: 'Foursome code required' });
    if (!verifySlotCode(state, dayKey, slotIndex, code)) return res.status(403).json({ error: 'Invalid foursome code' });
    const view = submitScorecard(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scorecard_submit', 'Tin Cup live scorecard submitted', {
      dayKey,
      slotIndex,
      scorerName: payload.scorerName,
    });
    return res.json(view);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/scramble/hole', async (req, res) => {
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    if (!isLiveScoringEnabled(state)) return res.status(403).json({ error: 'Live foursome scoring is disabled for this trip.' });
    const scramble = updateScrambleHoleScore(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scramble_score', 'Tin Cup scramble hole updated', {
      teamIndex: payload.teamIndex,
      hole: payload.hole,
      gross: payload.gross,
      scorerName: payload.scorerName,
    });
    return res.json({ scramble });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/admin/codes', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    const meta = getLiveMeta(state);
    return res.json({
      message: 'Existing code status by foursome.',
      daySlots: meta.daySlots,
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/admin/codes', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    if (!state.settings.enableFoursomeCodes) return res.status(400).json({ error: 'Foursome code feature is disabled for this trip.' });
    const body = req.body || {};
    const dayKey = String(body.dayKey || '').trim();
    const slotIndex = Number(body.slotIndex);
    const force = body.force === true;
    const generated = [];
    const daySlots = getLiveMeta(state).daySlots;
    const targets = [];
    daySlots.forEach((day) => {
      if (dayKey && day.dayKey !== dayKey) return;
      day.slots.forEach((slot) => {
        if (Number.isInteger(slotIndex) && slot.slotIndex !== slotIndex) return;
        targets.push({ dayKey: day.dayKey, slotIndex: slot.slotIndex });
      });
    });
    targets.forEach((target) => {
      const hasCode = Boolean(state.codes[`${target.dayKey}|${target.slotIndex}`]);
      if (hasCode && !force) return;
      const created = setSlotCode(state, target.dayKey, target.slotIndex, body.code || '');
      generated.push({
        dayKey: target.dayKey,
        slotIndex: target.slotIndex,
        label: created.slot.label,
        players: created.slot.players.map((player) => player.name),
        code: created.code,
      });
    });
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_codes', 'Tin Cup foursome codes generated', {
      generatedCount: generated.length,
      dayKey: dayKey || null,
      slotIndex: Number.isInteger(slotIndex) ? slotIndex : null,
      force,
    });
    return res.json({
      generatedCount: generated.length,
      generated,
      note: generated.length ? 'Store these codes securely. Codes are not returned again unless regenerated.' : 'No codes generated (already existed).',
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/admin/scramble-bonus', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    setScrambleBonus(state, payload.playerName, payload.value);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scramble_bonus', 'Tin Cup scramble bonus updated', {
      playerName: payload.playerName,
      value: payload.value,
    });
    return res.json({ scrambleBonus: state.scrambleBonus });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/admin/penalty', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    const penalties = setPlayerPenalty(state, payload.playerName, {
      champion: payload.champion,
      rookie: payload.rookie,
    });
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_penalty', 'Tin Cup player penalty updated', {
      playerName: payload.playerName,
      champion: payload.champion,
      rookie: payload.rookie,
    });
    return res.json({ penalties });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});


router.put('/:tripId/tin-cup/live/admin/scramble-score', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    const scramble = updateScrambleHoleScore(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_scramble_score', 'Tin Cup scramble hole updated', {
      teamIndex: payload.teamIndex,
      hole: payload.hole,
      gross: payload.gross,
    });
    return res.json({ scramble });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/admin/side-game', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    const sideGames = updateSideGameWinner(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_side_game', 'Tin Cup side game winner updated', {
      type: payload.type,
      dayKey: payload.dayKey,
      winner: payload.winner,
    });
    return res.json({ sideGames });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/admin/side-game/secret-snowman', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    const result = pickSecretSnowmanWinner(state, payload);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_secret_snowman_draw', 'Tin Cup Secret Snowman winner drawn', {
      dayKey: payload.dayKey,
      winner: result.picked.playerName,
      slotIndex: result.picked.slotIndex,
      slotLabel: result.picked.label,
      hole: result.picked.hole,
      gross: result.picked.gross,
    });
    return res.json(result);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});


router.put('/:tripId/tin-cup/live/admin/config', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const payload = req.body || {};
    const state = ensureTinCupLiveState(trip);
    const config = updateWorkbookConfig(state, payload.config || {});
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_config', 'Tin Cup workbook configuration updated', {
      fields: Object.keys(payload.config || {}),
    });
    return res.json({ config });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/admin/seed-scores', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    const reset = !(req.body && req.body.reset === false);
    const leaderboard = seedAllScores(state, { reset });
    const seedSummary = buildSeedSummary(state, leaderboard);
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_seed_scores', 'Tin Cup demo scores seeded for all rounds', {
      reset,
      scorecardCount: Object.keys(state.scorecards || {}).length,
    });
    const scorecardCount = Object.keys(state.scorecards || {}).length;
    const holeCount = Object.values(state.scorecards || {}).reduce((sum, card) => {
      const players = (card && card.players && typeof card.players === 'object') ? Object.values(card.players) : [];
      return sum + players.reduce((playerSum, player) => playerSum + (Array.isArray(player && player.holes) ? player.holes.filter((gross) => Number.isFinite(Number(gross))).length : 0), 0);
    }, 0);
    return res.json({
      message: 'Seeded Tin Cup scores for every round.',
      reset,
      scorecardCount,
      holeCount,
      topFive: (leaderboard.totals || []).slice(0, 5),
      seedSummary,
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.post('/:tripId/tin-cup/live/admin/clear-competition', async (req, res) => {
  if (!isDeleteAdmin(req)) return res.status(403).json({ error: 'Delete code required' });
  if (!hasDestructiveConfirm(req)) return res.status(403).json({ error: 'Destructive confirmation code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    const scorecardCount = Object.keys(state.scorecards || {}).length;
    const holeCount = Object.values(state.scorecards || {}).reduce((sum, card) => {
      const players = (card && card.players && typeof card.players === 'object') ? Object.values(card.players) : [];
      return sum + players.reduce((playerSum, player) => playerSum + (Array.isArray(player && player.holes) ? player.holes.filter((gross) => Number.isFinite(Number(gross))).length : 0), 0);
    }, 0);
    const scrambleHoleCount = Object.values(((state.scramble || {}).scores) || {}).reduce((sum, holes) => {
      const list = Array.isArray(holes) ? holes : [];
      return sum + list.filter((gross) => Number.isFinite(Number(gross))).length;
    }, 0);
    const sideGameWinnerCount = Object.values(state.sideGames || {}).reduce((sum, game) => {
      const days = (game && typeof game === 'object') ? Object.values(game) : [];
      return sum + days.filter((winner) => String(winner || '').trim()).length;
    }, 0);
    const preservedCodeCount = Object.keys(state.codes || {}).length;
    clearCompetitionState(state, {
      preserveCodes: true,
      preservePenalties: false,
      preserveConfig: true,
      preserveSettings: true,
    });
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_clear_competition', 'Tin Cup competition data cleared for a fresh start', {
      scorecardCount,
      holeCount,
      scrambleHoleCount,
      sideGameWinnerCount,
      preservedCodeCount,
    });
    return res.json({
      message: 'Cleared Tin Cup competition data. Player names and handicaps remain ready for Day 1, while settings, codes, and scoring config were preserved.',
      cleared: {
        scorecardCount,
        holeCount,
        scrambleHoleCount,
        sideGameWinnerCount,
      },
      preserved: {
        codeCount: preservedCodeCount,
      },
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/tin-cup/live/settings', async (req, res) => {
  try {
    const { trip } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    return res.json({ settings: state.settings });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.put('/:tripId/tin-cup/live/settings', async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
  try {
    const { trip, TripModel, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const state = ensureTinCupLiveState(trip);
    const next = updateSettings(state, req.body && req.body.settings ? req.body.settings : {});
    trip.markModified('tinCupLive');
    await TripModel.updateOne({ _id: trip._id }, { tinCupLive: trip.tinCupLive });
    await writeTripAudit(req, trip, TripAuditLogModel, 'tin_cup_settings', 'Tin Cup live settings changed', {
      settings: req.body && req.body.settings ? req.body.settings : {},
    });
    return res.json({ settings: next });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

router.get('/:tripId/audit-log', async (req, res) => {
  try {
    const { trip, TripAuditLogModel } = await loadTripBundle(req);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const rawLimit = Number(req.query.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(1000, Math.floor(rawLimit))) : 250;
    const rows = await TripAuditLogModel.find({ tripId: trip._id }).sort({ timestamp: 1 }).limit(limit).lean();
    const hasCreateEntry = rows.some((row) => String(row && row.action || '').trim() === 'trip_created');
    const createdRow = !hasCreateEntry && trip.createdAt ? {
      _id: `created-${String(trip._id)}`,
      tripId: trip._id,
      action: 'trip_created',
      actor: 'admin',
      method: 'CREATE',
      route: `/api/trips/${trip._id}`,
      summary: 'Trip created',
      details: {
        name: trip.name || '',
        location: trip.location || '',
      },
      timestamp: trip.createdAt,
    } : null;
    const auditRows = createdRow ? [createdRow, ...rows] : rows;
    return res.json({
      tripId: String(trip._id),
      startedAt: getTripStartDate(trip),
      count: auditRows.length,
      rows: auditRows,
    });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

// List participants
router.get('/:tripId/participants', async (req, res) => {
  if (req.query.myrtleBeach2026 === 'true') {
    const { TripParticipantSecondary } = getSecondaryModels();
    if (TripParticipantSecondary) {
      const participants = await TripParticipantSecondary.find({ trip: req.params.tripId });
      return res.json(participants);
    }
  }
  const participants = await TripParticipantPrimary.find({ trip: req.params.tripId });
  res.json(participants);
});

// Add participant
router.post('/:tripId/participants', async (req, res) => {
  try {
    const { TripModel, TripParticipantModel, TripAuditLogModel } = getTripModelsForRequest(req);
    const trip = await TripModel.findById(req.params.tripId);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const participant = await TripParticipantModel.create({ ...req.body, trip: req.params.tripId });
    await writeTripAudit(req, trip, TripAuditLogModel, 'participant_add', 'Trip participant added', {
      participantId: participant._id,
      name: participant.name,
      status: participant.status,
    });
    return res.json(participant);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

// Update participant
router.put('/:tripId/participants/:participantId', async (req, res) => {
  const needsAdmin = ['status', 'totalPaidAmount', 'depositPaid', 'fullAmountPaid', 'handicapIndex'].some((k) => k in req.body);
  if (needsAdmin && !isAdmin(req)) {
    return res.status(403).json({ error: 'Admin code required' });
  }
  try {
    const { TripModel, TripParticipantModel, TripAuditLogModel } = getTripModelsForRequest(req);
    const trip = await TripModel.findById(req.params.tripId);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const participantBefore = await TripParticipantModel.findById(req.params.participantId);
    const participant = await TripParticipantModel.findByIdAndUpdate(req.params.participantId, req.body, { new: true });
    if (!participant) return res.status(404).json({ error: 'Participant not found' });
    await writeTripAudit(req, trip, TripAuditLogModel, 'participant_update', 'Trip participant updated', {
      participantId: participant._id,
      name: participant.name,
      fields: Object.keys(req.body || {}),
      before: participantBefore ? {
        name: participantBefore.name,
        status: participantBefore.status,
        handicapIndex: participantBefore.handicapIndex,
        roomAssignment: participantBefore.roomAssignment,
      } : null,
      after: {
        name: participant.name,
        status: participant.status,
        handicapIndex: participant.handicapIndex,
        roomAssignment: participant.roomAssignment,
      },
    });
    return res.json(participant);
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

// Delete participant
router.delete('/:tripId/participants/:participantId', async (req, res) => {
  if (!isDeleteAdmin(req)) {
    return res.status(403).json({ error: 'Delete code required' });
  }
  try {
    const { TripModel, TripParticipantModel, TripAuditLogModel } = getTripModelsForRequest(req);
    const trip = await TripModel.findById(req.params.tripId);
    if (!trip) return res.status(404).json({ error: 'Trip not found' });
    const participant = await TripParticipantModel.findById(req.params.participantId);
    await TripParticipantModel.findByIdAndDelete(req.params.participantId);
    await writeTripAudit(req, trip, TripAuditLogModel, 'participant_delete', 'Trip participant removed', {
      participantId: req.params.participantId,
      name: participant && participant.name ? participant.name : '',
    });
    return res.json({ ok: true });
  } catch (error) {
    return sendTripRouteError(res, error);
  }
});

module.exports = router;

