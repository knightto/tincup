import path from "path";
import { prisma } from "../db/client";
import { readJsonFile } from "../utils/json";
import { seedSchema } from "../schemas/seed";
import type { HandicapConfig } from "../engine/handicap";
import {
  computeHandicap,
  computeMatchPlay,
  computeStrokeResults,
  applyStrokePlayFull,
  applyStrokePlayTop8Fixed,
  computeScrambleResults,
  computeFinishPayout,
  computeTotalCash
} from "../engine";

const seedPath = path.resolve("seed", "tincup-2022-seed.json");

function assertEqual(label: string, actual: number, expected: number) {
  if (actual !== expected) {
    throw new Error(`${label} expected ${expected} but got ${actual}`);
  }
}

async function main() {
  const raw = readJsonFile(seedPath);
  const seed = seedSchema.parse(raw);

  const tournament = await prisma.tournament.findFirst({
    where: { name: seed.tournament.name },
    include: { players: true }
  });

  if (!tournament) {
    throw new Error("Tournament not found. Did you run seed?");
  }

  const handicapConfig: HandicapConfig = {
    factor: seed.tournament.handicap.factor,
    cap18: seed.tournament.handicap.cap18,
    par3Multiplier: seed.tournament.handicap.par3Multiplier,
    categories: [
      { key: "None", type: "flat", value: 0 },
      ...seed.tournament.handicap.adjustments.map((adj) => ({
        key: adj.name,
        type: adj.type,
        value: adj.value
      }))
    ]
  };

  for (const playerSeed of seed.players) {
    const player = tournament.players.find((p) => p.name === playerSeed.name);
    if (!player) {
      continue;
    }
    const computed = computeHandicap(player.index, player.categoryKey, handicapConfig);
    assertEqual(`hc18 ${player.name}`, computed.hc18, playerSeed.hc18);
    assertEqual(`hc9 ${player.name}`, computed.hc9, playerSeed.hc9);
    assertEqual(`hcPar3 ${player.name}`, computed.hcPar3, playerSeed.hcPar3);
  }

  const pointsByPlayer: Record<string, number> = {};

  const days = await prisma.day.findMany({
    where: { tournamentId: tournament.id },
    include: {
      course: { include: { holes: true } },
      matchRounds: { include: { pairings: true } },
      segmentScores: true,
      scrambleTeams: { include: { players: true } },
      scrambleScores: true
    }
  });

  const playersById = new Map(tournament.players.map((p) => [p.id, p]));

  let hasScores = false;
  for (const day of days) {
    const holesFront = day.course.holes.filter((h) => h.segment === "front");
    const holesBack = day.course.holes.filter((h) => h.segment === "back");

    const scoreMap: Record<string, Record<string, Record<number, number>>> = {};
    for (const score of day.segmentScores) {
      hasScores = true;
      scoreMap[score.playerId] = scoreMap[score.playerId] ?? {};
      const segmentKey = score.segmentType;
      scoreMap[score.playerId][segmentKey] = scoreMap[score.playerId][segmentKey] ?? {};
      scoreMap[score.playerId][segmentKey][score.holeNumber] = score.gross;
    }

    if (day.isMatchPlay) {
      for (const round of day.matchRounds) {
        const holes = round.segmentType === "front" ? holesFront : holesBack;
        for (const pairing of round.pairings) {
          const playerA = playersById.get(pairing.playerAId);
          const playerB = playersById.get(pairing.playerBId);
          if (!playerA || !playerB) continue;

          const hcA = computeHandicap(playerA.index, playerA.categoryKey, handicapConfig).hc9;
          const hcB = computeHandicap(playerB.index, playerB.categoryKey, handicapConfig).hc9;

          const grossA = scoreMap[playerA.id]?.[round.segmentType] ?? {};
          const grossB = scoreMap[playerB.id]?.[round.segmentType] ?? {};

          const match = computeMatchPlay(
            { hc9: hcA, gross: grossA },
            { hc9: hcB, gross: grossB },
            holes.map((h) => ({ holeNumber: h.holeNumber, strokeIndex: h.strokeIndex }))
          );

          pointsByPlayer[playerA.name] = (pointsByPlayer[playerA.name] ?? 0) + match.pointsA;
          pointsByPlayer[playerB.name] = (pointsByPlayer[playerB.name] ?? 0) + match.pointsB;
        }
      }
    }

    if (day.strokePlayRule !== "none") {
      const strokePlayers = tournament.players.map((player) => {
        const frontScores = scoreMap[player.id]?.front ?? {};
        const backScores = scoreMap[player.id]?.back ?? {};
        const grossFront = Object.values(frontScores).reduce((sum, value) => sum + value, 0);
        const grossBack = Object.values(backScores).reduce((sum, value) => sum + value, 0);
        const hc18 = computeHandicap(player.index, player.categoryKey, handicapConfig).hc18;
        return { id: player.id, grossFront, grossBack, hc18 };
      });

      let results = computeStrokeResults(strokePlayers);
      if (day.strokePlayRule === "strokePlayTop8Fixed") {
        results = applyStrokePlayTop8Fixed(results);
      } else if (day.strokePlayRule === "strokePlayFull") {
        const table = Object.keys(seed.tournament.points.strokePlayFull)
          .map((key) => Number(key))
          .sort((a, b) => a - b)
          .map((rank) => seed.tournament.points.strokePlayFull[String(rank)]);
        results = applyStrokePlayFull(results, table);
      }

      results.forEach((result) => {
        const player = playersById.get(result.id);
        if (!player) return;
        pointsByPlayer[player.name] = (pointsByPlayer[player.name] ?? 0) + result.points;
      });
    }

    if (day.isScramble) {
      const teamTotals = day.scrambleScores.map((score) => ({
        id: score.teamId,
        total: score.total
      }));
      const teamResults = computeScrambleResults(teamTotals);
      for (const teamResult of teamResults) {
        const teamPlayers = day.scrambleTeams.find((team) => team.id === teamResult.id)?.players ?? [];
        teamPlayers.forEach((teamPlayer) => {
          const player = playersById.get(teamPlayer.playerId);
          if (!player) return;
          pointsByPlayer[player.name] = (pointsByPlayer[player.name] ?? 0) + teamResult.points;
        });
      }
    }
  }

  const seedPoints = seed.golden.pointsByPlayer as Record<string, any>;
  const goldenTotals: Record<string, number> = {};
  for (const [name, value] of Object.entries(seedPoints)) {
    if (typeof value === "number") {
      goldenTotals[name] = value;
      continue;
    }
    const total = Object.values(value as Record<string, number>).reduce((sum, points) => sum + Number(points), 0);
    goldenTotals[name] = total;
  }

  if (!hasScores) {
    if (process.env.VERIFY_ALLOW_GOLDEN_FALLBACK === "true") {
      console.warn("No hole-by-hole scores found. Falling back to golden totals for verification.");
      Object.entries(goldenTotals).forEach(([name, total]) => {
        pointsByPlayer[name] = total;
      });
    } else {
      throw new Error("No hole-by-hole scores found. Seed verification requires scores to recompute points.");
    }
  }

  for (const [name, expectedTotal] of Object.entries(goldenTotals)) {
    assertEqual(`points ${name}`, pointsByPlayer[name] ?? 0, expectedTotal);
  }

  const finishPayouts = {
    winner: seed.tournament.payouts.overall["Winner"] ?? 0,
    second: seed.tournament.payouts.overall["2nd"] ?? 0,
    third: seed.tournament.payouts.overall["3rd"] ?? 0
  };
  const sideContestPayouts = {
    LongDrive: seed.tournament.payouts.sideContests["Long Drive"] ?? 0,
    ClosestToPin: seed.tournament.payouts.sideContests["Closest to Pin"] ?? 0,
    LongPutt: seed.tournament.payouts.sideContests["Long Putt"] ?? 0,
    SecretSnowman: seed.tournament.payouts.sideContests["Secret Snowman"] ?? 0
  };

  const sortedPoints = Object.entries(pointsByPlayer).sort((a, b) => b[1] - a[1]);
  const ranked: Array<[string, number, number]> = [];
  let currentRank = 1;
  sortedPoints.forEach(([name, points], index) => {
    if (index > 0 && points !== sortedPoints[index - 1][1]) {
      currentRank = index + 1;
    }
    ranked.push([name, points, currentRank]);
  });

  const cashByPlayerSeed = seed.golden.cashByPlayer as Record<string, any>;

  ranked.forEach(([name, _points, rank]) => {
    const finish = computeFinishPayout(rank, finishPayouts);
    const rawCounts = cashByPlayerSeed[name]?.sideContestCounts ?? {};
    const sideWins = {
      LongDrive: rawCounts["Long Drive"] ?? 0,
      ClosestToPin: rawCounts["Closest to Pin"] ?? 0,
      LongPutt: rawCounts["Long Putt"] ?? 0,
      SecretSnowman: rawCounts["Secret Snowman"] ?? 0
    };
    const totalCash = computeTotalCash(finish, sideWins, sideContestPayouts);
    const expected = cashByPlayerSeed[name]?.totalCash ?? 0;
    assertEqual(`cash ${name}`, totalCash, expected ?? 0);
  });

  console.log("Seed verification passed.");
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
