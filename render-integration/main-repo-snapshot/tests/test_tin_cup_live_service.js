const assert = require('assert');
const {
  defaultTinCupLiveState,
  clearCompetitionState,
  getScorecardView,
  buildLeaderboard,
  buildDayRows,
  setSlotCode,
  verifySlotCode,
  setScorecardScorer,
  updateHoleScore,
  updateMarker,
  submitScorecard,
  seedAllScores,
  getScrambleResults,
  buildSkinsResults,
  buildScoreRankings,
  buildHandicapSummary,
  buildPayoutSummary,
  buildSeedSummary,
  buildWorkbookResultsAudit,
  buildCompetitionExportRows,
  buildCompetitionExportCsv,
  updateScrambleHoleScore,
  updateWorkbookConfig,
  setPlayerPenalty,
  pickSecretSnowmanWinner,
  maybeAutoSubmitScorecard,
  maybeAutoDrawSecretSnowman,
} = require('../services/tinCupLiveService');

function fillCompleteDay(state, dayKey, defaultGross = 5, overrides = []) {
  const overrideMap = new Map(overrides.map((row) => [`${row.slotIndex}|${row.playerName}|${row.hole}`, row.gross]));
  for (let slotIndex = 0; slotIndex < 4; slotIndex += 1) {
    const view = getScorecardView(state, dayKey, slotIndex);
    view.players.forEach((player) => {
      for (let hole = 1; hole <= 18; hole += 1) {
        const key = `${slotIndex}|${player.name}|${hole}`;
        const gross = overrideMap.has(key) ? overrideMap.get(key) : defaultGross;
        updateHoleScore(state, { dayKey, slotIndex, playerName: player.name, hole, gross });
      }
    });
  }
}

function run() {
  const workbookStrokeState = defaultTinCupLiveState();
  updateHoleScore(workbookStrokeState, { dayKey: 'Day 1', slotIndex: 0, playerName: 'Matt', hole: 1, gross: 5 });
  updateHoleScore(workbookStrokeState, { dayKey: 'Day 1', slotIndex: 0, playerName: 'Matt', hole: 2, gross: 5 });
  const workbookStrokeBoard = buildLeaderboard(workbookStrokeState);
  const front9Matches = ((((workbookStrokeBoard.matchDetails || {})['Day 1'] || [])[0] || { segments: [] }).segments[0] || { matches: [] }).matches || [];
  const mattFront9Match = front9Matches.find((match) => match.left === 'Matt' && match.right === 'Tommy');
  assert(mattFront9Match, 'Updated Day 1 front-nine pairing should expose Matt vs Tommy');
  assert.strictEqual(mattFront9Match.holes[0].leftNet, 5, 'Day 1 hole 1 should use the workbook stroke index so Matt gets no stroke there');
  assert.strictEqual(mattFront9Match.holes[1].leftNet, 4, 'Day 1 hole 2 should use the workbook stroke index so Matt gets one stroke there');
  const setupState = defaultTinCupLiveState();
  const day1Group1 = getScorecardView(setupState, 'Day 1', 0);
  const day2AGroup1 = getScorecardView(setupState, 'Day 2A', 0);
  const day2BGroup4 = getScorecardView(setupState, 'Day 2B', 3);
  const day3Group2 = getScorecardView(setupState, 'Day 3', 1);
  assert.deepStrictEqual(day1Group1.players.map((player) => `${player.name}:${player.handicap}`), ['OB:7', 'Rick:8', 'Matt:6', 'Tommy:5'], 'Day 1 Group 1 should match the updated workbook foursome and nine-hole handicaps');
  assert.deepStrictEqual(day2AGroup1.players.map((player) => `${player.name}:${player.handicap}`), ['Paul O:7', 'Tommy:5', 'Mil:8', 'Spiro:14'], 'Day 2A Group 1 should map to the workbook DNP rotation foursome');
  assert.deepStrictEqual(day2BGroup4.players.map((player) => `${player.name}:${player.handicap}`), ['John:7', 'Rick:8', 'Paul O:7', 'Brian:12'], 'Day 2B Group 4 should match the updated Mid Pines foursome');
  assert.deepStrictEqual(day3Group2.players.map((player) => `${player.name}:${player.handicap}`), ['OB:7', 'Paul O:7', 'Kyle:4', 'Manny:13'], 'Day 3 Group 2 should match the updated Tobacco Road foursome');
  assert.deepStrictEqual(getScrambleResults(setupState).teams.map((team) => `${team.label}:${team.players.join('|')}`), [
    'Team 1:Paul O|David|Matt|Bob',
    'Team 2:Rick|Kyle|Tony|Spiro',
    'Team 3:Tommy|John|Manny|Pat',
    'Team 4:Mil|OB|Steve|Brian',
  ], 'Scramble teams should use the workbook scramble groups instead of the Day 1 foursomes');

  const state = defaultTinCupLiveState();
  const leaderboard = seedAllScores(state, { reset: true });

  assert.strictEqual(Object.keys(state.scorecards || {}).length, 20, 'Seeding should populate all 20 competitive Tin Cup scorecards');
  const holeCount = Object.values(state.scorecards || {}).reduce((sum, card) => {
    const players = card && card.players && typeof card.players === 'object' ? Object.values(card.players) : [];
    return sum + players.reduce((playerSum, player) => playerSum + (Array.isArray(player && player.holes) ? player.holes.filter((gross) => Number.isFinite(Number(gross))).length : 0), 0);
  }, 0);
  assert.strictEqual(holeCount, 1440, 'Seeding should create 1,440 gross scores across competitive rounds');

  assert.strictEqual((leaderboard.totals || []).length, 16, 'Leaderboard should contain all 16 Tin Cup players');
  assert((leaderboard.totals || []).some((row) => row.name === 'Paul O'), 'Leaderboard should include Paul O after the workbook update');
  assert(!(leaderboard.totals || []).some((row) => row.name === 'CSamm'), 'Leaderboard should drop the removed player after the workbook update');
  assert.strictEqual(leaderboard.matchBoards['Day 1'].length, 16, 'Day 1 match board should contain all players');
  assert.strictEqual(leaderboard.matchBoards['Practice'].length, 0, 'Practice match board should stay empty until practice groups are assigned');

  const matt = leaderboard.totals.find((row) => row.name === 'Matt');
  const spiro = leaderboard.totals.find((row) => row.name === 'Spiro');
  assert(matt, 'Matt leaderboard row should exist');
  assert(spiro, 'Spiro leaderboard row should exist');
  assert.strictEqual(matt.penaltyTotal, 2, 'Matt seed penalty should be reflected in totals');
  assert.strictEqual(spiro.penaltyTotal, 1, 'Spiro seed penalty should be reflected in totals');
  assert.notStrictEqual(matt.day1Net, null, 'Seeded stroke totals should populate Day 1 net totals');
  assert.notStrictEqual(spiro.day4Net, null, 'Seeded stroke totals should populate Day 4 net totals');
  assert(leaderboard.scramble && Array.isArray(leaderboard.scramble.teams) && leaderboard.scramble.teams.length === 4, 'Seeded leaderboard should expose scramble teams');
  assert(leaderboard.skins && Array.isArray(leaderboard.skins.days) && leaderboard.skins.days.length >= 5, 'Seeded leaderboard should expose skins summaries');
  assert(leaderboard.sideGames && leaderboard.sideGames.longPutt && leaderboard.sideGames.secretSnowman, 'Seeded leaderboard should expose side game summaries');
  assert(leaderboard.payouts && Array.isArray(leaderboard.payouts.rows), 'Seeded leaderboard should expose payout summary');
  assert(Array.isArray(leaderboard.scoreRankings) && leaderboard.scoreRankings.length > 0, 'Seeded leaderboard should expose average score rankings');
  assert(Array.isArray(leaderboard.handicapSummary) && leaderboard.handicapSummary.length === 16, 'Seeded leaderboard should expose handicap summary rows');
  assert(Array.isArray(leaderboard.workbookResults) && leaderboard.workbookResults.length === 16, 'Seeded leaderboard should expose workbook audit rows');

  const scramble = getScrambleResults(state);
  assert(scramble.teams.every((team) => team.played === 18), 'Seeded scramble teams should have 18 entered holes');
  assert(scramble.teams.some((team) => team.rank === 1), 'Seeded scramble teams should rank teams automatically');

  const skins = buildSkinsResults(state);
  assert(skins.days.some((day) => day.skinCount > 0), 'Seeded skins data should produce at least one winning skin day');

  const rankings = buildScoreRankings(state);
  assert(rankings[0] && rankings[0].label === 'Winner', 'Average score rankings should label the winner');

  const handicapSummary = buildHandicapSummary(state);
  const mattHandicap = handicapSummary.find((row) => row.name === 'Matt');
  assert(mattHandicap && mattHandicap.eighteenHole > 0 && mattHandicap.par3 > 0, 'Handicap summary should include converted values');

  const payouts = buildPayoutSummary(state, leaderboard);
  assert.strictEqual(typeof payouts.balance, 'number', 'Payout summary should compute remaining balance');
  leaderboard.totals.forEach((row) => {
    assert.strictEqual(row.day1Total, row.match1 + row.stroke1, `${row.name} Day 1 total should equal match plus stroke points`);
    assert.strictEqual(row.day2Total, row.match2A + row.match2B, `${row.name} Day 2 total should equal the two match segments`);
    assert.strictEqual(row.day3Total, row.match3 + row.stroke3 + row.scramble, `${row.name} Day 3 total should equal match, stroke, and scramble points`);
    assert.strictEqual(row.total, Number((row.day1Total + row.day2Total + row.day3Total + row.day4Points).toFixed(2)), `${row.name} trip total should equal all day totals plus Day 4 rank points`);
  });
  [
    ['Day 1', (row) => row.day1Total],
    ['Day 2A', (row) => row.match2A],
    ['Day 2B', (row) => row.match2B],
    ['Day 2 Total', (row) => row.day2Total],
    ['Day 3', (row) => row.day3Total],
    ['Day 4', (row) => row.day4Points || 0],
  ].forEach(([dayKey, getter]) => {
    const rows = buildDayRows(leaderboard, dayKey);
    rows.forEach((dayRow) => {
      const totalRow = leaderboard.totals.find((entry) => entry.name === dayRow.name);
      assert(totalRow, `${dayKey} row should map back to a leaderboard total for ${dayRow.name}`);
      assert.strictEqual(dayRow.points, getter(totalRow), `${dayKey} row points should match the leaderboard breakdown for ${dayRow.name}`);
      assert.strictEqual(dayRow.total, totalRow.total, `${dayKey} row trip total should match the leaderboard total for ${dayRow.name}`);
    });
  });
  payouts.rows.forEach((row) => {
    const expectedTotal = Math.round(
      Number(row.finalPrize || 0)
      + Number(row.ctp || 0)
      + Number(row.longDrive || 0)
      + Number(row.longPutt || 0)
      + Number(row.secretSnowman || 0)
      + Number(row.skins || 0)
      + Number(row.loser || 0)
    );
    assert.strictEqual(row.total, expectedTotal, `Payout row total should equal the component sums for ${row.name}`);
  });
  assert.strictEqual(payouts.distributed, payouts.rows.reduce((sum, row) => sum + Number(row.total || 0), 0), 'Distributed payout total should equal the sum of payout rows');
  assert.strictEqual(payouts.pot, payouts.mainPot + payouts.skinsPot, 'Combined payout pot should equal entry plus skins pots');
  assert.strictEqual(payouts.balance, payouts.pot - payouts.distributed, 'Payout balance should equal combined pot minus distributed amount');
  const seedSummary = buildSeedSummary(state, leaderboard);
  assert.strictEqual((seedSummary.longPutt.days || []).length, 6, 'Seed summary should include Long Putt winners for every configured day');
  assert((seedSummary.longPutt.days || []).every((row) => row.winner), 'Seed summary should assign every Long Putt winner');
  assert.strictEqual((seedSummary.secretSnowman.days || []).length, 5, 'Seed summary should include Secret Snowman winners for every configured day');
  assert((seedSummary.secretSnowman.days || []).every((row) => row.winner), 'Seed summary should assign every Secret Snowman winner');
  assert.strictEqual((seedSummary.markerTotals.ctp || []).reduce((sum, row) => sum + row.wins, 0), 20, 'Seed summary should seed all 20 Closest To Pin markers across competitive rounds');
  assert.strictEqual((seedSummary.markerTotals.longDrive || []).reduce((sum, row) => sum + row.wins, 0), 10, 'Seed summary should seed all 10 Long Drive markers across competitive rounds');
  assert((seedSummary.scramble.teams || []).every((team) => team.rank !== null), 'Seed summary should include ranked scramble standings');
  assert(Array.isArray(seedSummary.skins.totals) && seedSummary.skins.totals.length > 0, 'Seed summary should include seeded skins winners');
  assert(seedSummary.loser && seedSummary.loser.name, 'Seed summary should include the seeded loser payout');
  assert((seedSummary.payoutRows || []).some((row) => row.ctp > 0), 'Seed summary should include seeded Closest To Pin payouts');
  assert((seedSummary.payoutRows || []).some((row) => row.longDrive > 0), 'Seed summary should include seeded Long Drive payouts');
  assert((seedSummary.payoutRows || []).some((row) => row.longPutt > 0), 'Seed summary should include seeded Long Putt payouts');
  assert((seedSummary.payoutRows || []).some((row) => row.secretSnowman > 0), 'Seed summary should include seeded Secret Snowman payouts');
  assert((seedSummary.payoutRows || []).some((row) => row.skins > 0), 'Seed summary should include seeded skins payouts');
  assert((seedSummary.payoutRows || []).some((row) => row.loser > 0), 'Seed summary should include the seeded loser payout row');

  const mattWorkbookRow = leaderboard.workbookResults.find((row) => row.name === 'Matt');
  assert(mattWorkbookRow && typeof mattWorkbookRow.ctpWins === 'number' && typeof mattWorkbookRow.longDriveWins === 'number', 'Workbook audit rows should include marker counts');

  assert.throws(() => {
    pickSecretSnowmanWinner(defaultTinCupLiveState(), { dayKey: 'Day 1' });
  }, /All scores for Day 1 must be entered/, 'Secret Snowman draw should require a fully entered day');

  const noEightState = defaultTinCupLiveState();
  fillCompleteDay(noEightState, 'Day 1', 5);
  assert.throws(() => {
    pickSecretSnowmanWinner(noEightState, { dayKey: 'Day 1' });
  }, /No score of 8 was entered on Day 1/, 'Secret Snowman draw should require at least one score of 8');

  const snowmanState = defaultTinCupLiveState();
  fillCompleteDay(snowmanState, 'Day 1', 5, [{ slotIndex: 0, playerName: 'Matt', hole: 7, gross: 8 }]);
  const snowmanDraw = pickSecretSnowmanWinner(snowmanState, { dayKey: 'Day 1' });
  assert.strictEqual(snowmanDraw.picked.playerName, 'Matt', 'Secret Snowman draw should pick the player attached to the selected 8');
  assert.strictEqual(snowmanDraw.picked.label, 'Group 1', 'Secret Snowman draw should report the foursome label');
  assert.strictEqual(snowmanDraw.picked.hole, 7, 'Secret Snowman draw should report the winning hole');
  const snowmanDayRow = (snowmanDraw.sideGames.secretSnowman.days || []).find((row) => row.dayKey === 'Day 1');
  assert(snowmanDayRow && snowmanDayRow.winner === 'Matt', 'Secret Snowman draw should persist the winner into side-game state');

  const autoSubmitState = defaultTinCupLiveState();
  fillCompleteDay(autoSubmitState, 'Day 1', 5);
  const autoSubmit = maybeAutoSubmitScorecard(autoSubmitState, { dayKey: 'Day 1', slotIndex: 0, scorerName: 'Rick' });
  assert.strictEqual(autoSubmit.autoSubmitted, true, 'Completed scorecards should auto-submit');
  assert.strictEqual(autoSubmit.view.submitted, true, 'Auto-submitted scorecards should come back locked');
  assert.strictEqual(autoSubmit.view.submittedBy, 'Rick', 'Auto-submitted scorecards should use the active scorer name');

  const autoSnowmanState = defaultTinCupLiveState();
  fillCompleteDay(autoSnowmanState, 'Day 1', 5, [{ slotIndex: 2, playerName: 'Steve', hole: 12, gross: 8 }]);
  const autoSnowman = maybeAutoDrawSecretSnowman(autoSnowmanState, { dayKey: 'Day 1' });
  assert.strictEqual(autoSnowman.autoDrawn, true, 'Completed day with an 8 should auto-draw Secret Snowman');
  assert(autoSnowman.picked && autoSnowman.picked.playerName === 'Steve', 'Auto-drawn Secret Snowman should report the matched player');
  const autoSnowmanDayRow = (autoSnowman.sideGames.secretSnowman.days || []).find((row) => row.dayKey === 'Day 1');
  assert(autoSnowmanDayRow && autoSnowmanDayRow.winner === 'Steve', 'Auto-drawn Secret Snowman should persist into side-game state');

  const mattBeforePenaltyRemoval = leaderboard.totals.find((row) => row.name === 'Matt');
  setPlayerPenalty(state, 'Matt', { champion: 0, rookie: 0 });
  const noPenaltyBoard = buildLeaderboard(state);
  const mattAfterPenaltyRemoval = noPenaltyBoard.totals.find((row) => row.name === 'Matt');
  assert(mattBeforePenaltyRemoval && mattAfterPenaltyRemoval, 'Matt leaderboard rows should exist before and after penalty changes');
  assert.strictEqual(mattAfterPenaltyRemoval.day1Net, mattBeforePenaltyRemoval.day1Net - 2, 'Removing Matt penalty should lower Day 1 adjusted net by two strokes');
  assert.strictEqual(mattAfterPenaltyRemoval.day3Net, mattBeforePenaltyRemoval.day3Net - 2, 'Removing Matt penalty should lower Day 3 adjusted net by two strokes');
  assert.strictEqual(mattAfterPenaltyRemoval.day4Net, mattBeforePenaltyRemoval.day4Net - 2, 'Removing Matt penalty should lower Day 4 adjusted net by two strokes');
  assert(mattAfterPenaltyRemoval.total >= mattBeforePenaltyRemoval.total, 'Removing a penalty should not reduce Matt total points');
  setPlayerPenalty(state, 'Matt', { champion: 2, rookie: 0 });

  const updatedConfig = updateWorkbookConfig(state, {
    accounting: { entryFee: 225, markerPayouts: { ctp: 30 } },
    handicap: { maxHandicap: 30 }
  });
  assert.strictEqual(updatedConfig.accounting.entryFee, 225, 'Workbook config updates should persist entry fee changes');
  assert.strictEqual(updatedConfig.accounting.markerPayouts.ctp, 30, 'Workbook config updates should merge nested marker payouts');
  assert.strictEqual(updatedConfig.accounting.markerPayouts.longPutt, 25, 'Workbook config should retain long putt defaults when not overridden');
  assert.strictEqual(updatedConfig.handicap.maxHandicap, 30, 'Workbook config updates should persist handicap config changes');

  const recalculatedPayouts = buildPayoutSummary(state);
  assert.strictEqual(recalculatedPayouts.mainPot, 3600, 'Updated entry fee should flow into payout pot calculations');
  const anyLongPuttPayout = recalculatedPayouts.rows.some((row) => Number(row.longPutt || 0) >= 25);
  assert(anyLongPuttPayout, 'Long putt winnings should flow into payout rows');
  const anySnowmanAudit = leaderboard.workbookResults.some((row) => Number(row.secretSnowmanWins || 0) >= 1);
  assert(anySnowmanAudit, 'Workbook audit rows should include secret snowman wins');

  updateWorkbookConfig(state, {
    accounting: { scramblePoints: [10, 5, 1, 0] }
  });
  const customScrambleBoard = buildLeaderboard(state);
  const configuredScramblePoints = [10, 5, 1, 0];
  const completeTeams = (customScrambleBoard.scramble.teams || [])
    .filter((team) => team.total !== null)
    .sort((a, b) => a.total - b.total || a.teamIndex - b.teamIndex);
  let scanIndex = 0;
  while (scanIndex < completeTeams.length) {
    const start = scanIndex;
    const score = completeTeams[scanIndex].total;
    while (scanIndex < completeTeams.length && completeTeams[scanIndex].total === score) scanIndex += 1;
    const firstRank = start + 1;
    let totalPoints = 0;
    for (let pos = firstRank; pos <= scanIndex; pos += 1) totalPoints += configuredScramblePoints[pos - 1] || 0;
    const expectedPoints = Number((totalPoints / (scanIndex - start)).toFixed(2));
    completeTeams.slice(start, scanIndex).forEach((team) => {
      assert.strictEqual(team.rank, firstRank, `Scramble rank should be assigned consistently for ${team.label}`);
      assert.strictEqual(team.points, expectedPoints, `Configured scramble points should apply to ${team.label}`);
      (team.players || []).forEach((name) => {
        const row = customScrambleBoard.totals.find((entry) => entry.name === name);
        assert(row && row.scramble === expectedPoints, `Scramble points should flow into ${name}'s leaderboard row`);
      });
    });
  }

  const payoutAuditBoard = buildLeaderboard(state);
  payoutAuditBoard.payouts = buildPayoutSummary(state, payoutAuditBoard);
  const payoutAuditRows = buildWorkbookResultsAudit(state, payoutAuditBoard);
  payoutAuditRows.forEach((auditRow) => {
    const payoutRow = payoutAuditBoard.payouts.rows.find((row) => row.name === auditRow.name);
    const leaderboardRow = payoutAuditBoard.totals.find((row) => row.name === auditRow.name);
    assert(leaderboardRow, `Leaderboard row should exist for ${auditRow.name}`);
    assert.strictEqual(auditRow.tripTotal, leaderboardRow.total, `Workbook audit should mirror trip total for ${auditRow.name}`);
    assert.strictEqual(auditRow.scramblePoints, leaderboardRow.scramble, `Workbook audit should mirror scramble points for ${auditRow.name}`);
    assert.strictEqual(auditRow.payoutTotal, payoutRow ? payoutRow.total : 0, `Workbook audit should mirror payout total for ${auditRow.name}`);
  });

  const currentExportBoard = buildLeaderboard(state);
  const currentMatt = currentExportBoard.totals.find((row) => row.name === 'Matt');
  const exportRows = buildCompetitionExportRows(state, {
    tripId: 'trip-123',
    tripName: 'Tin Cup 2026',
    tripStartDate: '2026-03-01T00:00:00.000Z',
    tripEndDate: '2026-03-05T23:59:59.999Z',
    exportedAt: '2026-03-06T12:00:00.000Z',
  });
  const summaryRow = exportRows.find((row) => row.rowType === 'trip_summary');
  assert(summaryRow && summaryRow.tripName === 'Tin Cup 2026', 'Competition export should include a trip summary row');
  const mattExport = exportRows.find((row) => row.rowType === 'player_total' && row.playerName === 'Matt');
  assert(mattExport && currentMatt && mattExport.tripTotal === currentMatt.total, 'Competition export should include per-player final totals');
  const practiceExport = exportRows.find((row) => row.rowType === 'match_day' && row.dayKey === 'Practice');
  assert.strictEqual(practiceExport, undefined, 'Competition export should omit practice match rows until practice groups are assigned');
  const mattScorecardExport = exportRows.find((row) => row.rowType === 'scorecard_player' && row.dayKey === 'Day 1' && row.playerName === 'Matt');
  assert(mattScorecardExport && Object.prototype.hasOwnProperty.call(mattScorecardExport, 'hole18'), 'Competition export should include full hole-by-hole scorecard rows');
  const markerExport = exportRows.find((row) => row.rowType === 'marker' && row.type === 'ctp');
  assert(markerExport, 'Competition export should include marker winner rows');
  const sideGameExport = exportRows.find((row) => row.rowType === 'side_game' && row.type === 'secretSnowman' && row.dayKey === 'Day 1');
  assert(sideGameExport && sideGameExport.winner, 'Competition export should include side-game winner rows');
  const scrambleExport = exportRows.find((row) => row.rowType === 'scramble_team' && row.teamLabel === 'Team 1');
  assert(scrambleExport && Object.prototype.hasOwnProperty.call(scrambleExport, 'hole18'), 'Competition export should include scramble team rows');
  const skinsExport = exportRows.find((row) => row.rowType === 'skin_hole' && row.hasSkin === true);
  assert(skinsExport, 'Competition export should include skins hole rows');

  const exportCsv = buildCompetitionExportCsv(state, {
    tripId: 'trip-123',
    tripName: 'Tin Cup 2026',
    tripStartDate: '2026-03-01T00:00:00.000Z',
    tripEndDate: '2026-03-05T23:59:59.999Z',
    exportedAt: '2026-03-06T12:00:00.000Z',
  });
  assert(exportCsv.includes('player_total'), 'Competition export CSV should include final leaderboard rows');
  assert(exportCsv.includes('scorecard_player'), 'Competition export CSV should include hole-by-hole scorecard rows');
  assert(exportCsv.includes('scramble_team'), 'Competition export CSV should include scramble rows');

  const clearedState = defaultTinCupLiveState();
  clearedState.settings.enableLiveLeaderboard = false;
  clearedState.settings.enableLiveMarkers = false;
  updateWorkbookConfig(clearedState, { accounting: { entryFee: 250 } });
  const retainedCode = setSlotCode(clearedState, 'Day 1', 0, 'BIRD').code;
  seedAllScores(clearedState, { reset: true });
  setPlayerPenalty(clearedState, 'Matt', { champion: 4, rookie: 0 });
  const preClearHoleCount = Object.values(clearedState.scorecards || {}).reduce((sum, card) => {
    const players = (card && card.players && typeof card.players === 'object') ? Object.values(card.players) : [];
    return sum + players.reduce((playerSum, player) => playerSum + (Array.isArray(player && player.holes) ? player.holes.filter((gross) => Number.isFinite(Number(gross))).length : 0), 0);
  }, 0);
  assert(preClearHoleCount > 0, 'Clear-competition test should start with seeded hole scores');
  clearCompetitionState(clearedState, {
    preserveCodes: true,
    preservePenalties: false,
    preserveConfig: true,
    preserveSettings: true,
  });
  assert.strictEqual(Object.keys(clearedState.scorecards || {}).length, 0, 'Clearing competition should remove all saved scorecards');
  assert.strictEqual(Object.keys((clearedState.scramble || {}).scores || {}).length, 0, 'Clearing competition should remove scramble hole scores');
  assert.strictEqual(Object.keys(clearedState.scrambleBonus || {}).length, 0, 'Clearing competition should remove scramble bonus fallbacks');
  assert(verifySlotCode(clearedState, 'Day 1', 0, retainedCode), 'Clearing competition should preserve generated foursome codes');
  assert.strictEqual(clearedState.settings.enableLiveLeaderboard, false, 'Clearing competition should preserve live settings');
  assert.strictEqual(clearedState.settings.enableLiveMarkers, false, 'Clearing competition should preserve live settings');
  assert.strictEqual(clearedState.config.accounting.entryFee, 250, 'Clearing competition should preserve workbook config');
  const mattAfterClear = buildHandicapSummary(clearedState).find((row) => row.name === 'Matt');
  assert(mattAfterClear && mattAfterClear.totalPenalty === 0, 'Clearing competition should clear seeded golfer penalties');
  const clearedSideGames = buildLeaderboard(clearedState).sideGames;
  assert((clearedSideGames.longPutt.days || []).every((row) => !row.winner), 'Clearing competition should remove Long Putt winners');
  assert((clearedSideGames.secretSnowman.days || []).every((row) => !row.winner), 'Clearing competition should remove Secret Snowman winners');

  const day3Rows = buildDayRows(leaderboard, 'Day 3');
  const mattDay3 = day3Rows.find((row) => row.name === 'Matt');
  assert(mattDay3, 'Matt day-row entry should exist');
  assert(/pen \+2/.test(mattDay3.detail), 'Penalty-adjusted day detail should include Matt\'s seeded penalty');

  const scorecard = getScorecardView(state, 'Day 1', 0);
  assert.strictEqual(scorecard.players.length, 4, 'Seeded scorecard view should expose foursome players');
  assert(scorecard.players.every((player) => player.complete18 === true), 'Seeded scorecards should be complete for every player');
  assert(scorecard.players.every((player) => Array.isArray(player.holes) && player.holes.length === 18), 'Each seeded player should have 18 holes');
  assert.deepStrictEqual(scorecard.markerHoles.ctp, [3, 7, 12, 17], 'Scorecard view should expose the allowed CTP holes');

  assert.throws(() => {
    updateMarker(state, { dayKey: 'Day 1', slotIndex: 0, type: 'ctp', hole: 5, winner: 'Matt' });
  }, /CTP is only allowed on par-3 holes/, 'Non-par-3 CTP holes should be rejected');

  const updatedScorecard = updateMarker(state, { dayKey: 'Day 1', slotIndex: 0, type: 'ctp', hole: 3, winner: 'Matt' });
  assert.strictEqual(updatedScorecard.markers.ctp['3'], 'Matt', 'Par-3 CTP holes should still save normally');
  const replacedScorecard = updateMarker(state, { dayKey: 'Day 1', slotIndex: 2, type: 'ctp', hole: 3, winner: 'Steve' });
  assert.strictEqual(replacedScorecard.markers.ctp['3'], 'Steve', 'Later CTP picks should replace earlier picks for the same day and hole');
  assert.strictEqual(getScorecardView(state, 'Day 1', 0).markers.ctp['3'], 'Steve', 'Replaced CTP winner should show on other scorecards for that day');
  assert.strictEqual((state.scorecards['Day 1|0'] && state.scorecards['Day 1|0'].markers && state.scorecards['Day 1|0'].markers.ctp['3']) || '', '', 'Previous scorecard should no longer keep the old CTP winner');

  const updatedLongDrive = updateMarker(state, { dayKey: 'Day 1', slotIndex: 0, type: 'longDrive', hole: 5, winner: 'Matt' });
  assert.strictEqual(updatedLongDrive.markers.longDrive['5'], 'Matt', 'Long drive should save normally');
  const replacedLongDrive = updateMarker(state, { dayKey: 'Day 1', slotIndex: 3, type: 'longDrive', hole: 5, winner: 'Brian' });
  assert.strictEqual(replacedLongDrive.markers.longDrive['5'], 'Brian', 'Later long-drive picks should replace earlier picks for the same day and hole');
  assert.strictEqual(getScorecardView(state, 'Day 1', 1).markers.longDrive['5'], 'Brian', 'Replaced long-drive winner should show across other scorecards for that day');

  const scrambleUpdate = updateScrambleHoleScore(state, { teamIndex: 0, hole: 1, gross: 3 });
  assert.strictEqual(scrambleUpdate.teams[0].holes[0], 3, 'Scramble hole updates should persist team hole scores');

  const opened = setScorecardScorer(state, { dayKey: 'Day 1', slotIndex: 0, scorerName: 'Rick' });
  assert.strictEqual(opened.scorerName, 'Rick', 'Scorecard should persist the golfer who opened it to keep score');

  const submitted = submitScorecard(state, { dayKey: 'Day 1', slotIndex: 0, scorerName: 'Matt' });
  assert.strictEqual(submitted.submitted, true, 'Submitted scorecards should be marked submitted');
  assert.strictEqual(submitted.submittedBy, 'Matt', 'Submitted scorecards should record the scorer name');
  assert(submitted.submittedAt, 'Submitted scorecards should record the submission timestamp');

  assert.throws(() => {
    updateHoleScore(state, { dayKey: 'Day 1', slotIndex: 0, playerName: 'Matt', hole: 18, gross: 4 });
  }, /Admin code required/, 'Submitted scorecards should reject normal edits');

  const adminEdited = updateHoleScore(state, {
    dayKey: 'Day 1',
    slotIndex: 0,
    playerName: 'Matt',
    hole: 18,
    gross: 4,
    allowSubmittedEdit: true,
  });
  assert.strictEqual((adminEdited.players || []).find((player) => player.name === 'Matt').holes[17], 4, 'Admin override should still allow submitted scorecard edits');

  console.log('test_tin_cup_live_service.js passed');
}

run();
