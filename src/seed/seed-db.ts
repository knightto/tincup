import path from "path";
import { existsSync } from "fs";
import { prisma } from "../db/client";
import { readJsonFile } from "../utils/json";
import { seedSchema } from "../schemas/seed";
import type { SeedData } from "../schemas/seed";

const seedPath = path.resolve("seed", "tincup-2022-seed.json");

export async function seedDatabase() {
  const raw = readJsonFile(seedPath);
  const seed = seedSchema.parse(raw);
  const scoresPath = path.resolve("seed", "tincup-2022-scores.json");
  const scoresData = existsSync(scoresPath)
    ? readJsonFile<Pick<SeedData, "scores">>(scoresPath)
    : null;
  const seedScores = scoresData?.scores ?? seed.scores ?? [];
  const playersWithScores = new Map<string, Set<string>>();
  for (const score of seedScores) {
    if (!score || score.gross === null || score.gross === undefined) continue;
    const key = `${score.day}|${score.segment}`;
    const set = playersWithScores.get(key) ?? new Set<string>();
    set.add(score.player);
    playersWithScores.set(key, set);
  }

  await prisma.$transaction([
    prisma.scrambleScore.deleteMany(),
    prisma.scrambleTeamPlayer.deleteMany(),
    prisma.scrambleTeam.deleteMany(),
    prisma.secretSnowmanDrawHistory.deleteMany(),
    prisma.secretSnowmanDraw.deleteMany(),
    prisma.sideContestResult.deleteMany(),
    prisma.daySideContestConfig.deleteMany(),
    prisma.matchPairing.deleteMany(),
    prisma.matchRound.deleteMany(),
    prisma.daySegmentScore.deleteMany(),
    prisma.day.deleteMany(),
    prisma.hole.deleteMany(),
    prisma.course.deleteMany(),
    prisma.sideContestDefinition.deleteMany(),
    prisma.player.deleteMany(),
    prisma.tournament.deleteMany()
  ]);

  const categories = [
    { key: "None", type: "flat", value: 0 },
    ...seed.tournament.handicap.adjustments.map((adj) => ({
      key: adj.name,
      type: adj.type,
      value: adj.value
    }))
  ];

  const strokePlayFullPoints = Object.keys(seed.tournament.points.strokePlayFull)
    .map((key) => Number(key))
    .sort((a, b) => a - b)
    .map((rank) => seed.tournament.points.strokePlayFull[String(rank)]);

  const tournament = await prisma.tournament.create({
    data: {
      name: seed.tournament.name,
      handicapFactor: seed.tournament.handicap.factor,
      handicapCap18: seed.tournament.handicap.cap18,
      par3Multiplier: seed.tournament.handicap.par3Multiplier,
      handicapCategories: JSON.stringify(categories),
      pointSchemes: JSON.stringify({
        strokePlayFullPoints,
        strokePlayTop8Fixed: seed.tournament.points.strokePlayTop8Fixed
      }),
      payouts: JSON.stringify({
        finish: {
          winner: seed.tournament.payouts.overall["Winner"] ?? 0,
          second: seed.tournament.payouts.overall["2nd"] ?? 0,
          third: seed.tournament.payouts.overall["3rd"] ?? 0
        },
        sideContests: {
          LongDrive: seed.tournament.payouts.sideContests["Long Drive"] ?? 0,
          ClosestToPin: seed.tournament.payouts.sideContests["Closest to Pin"] ?? 0,
          LongPutt: seed.tournament.payouts.sideContests["Long Putt"] ?? 0,
          SecretSnowman: seed.tournament.payouts.sideContests["Secret Snowman"] ?? 0
        }
      })
    }
  });

  await prisma.player.createMany({
    data: seed.players.map((player) => ({
      tournamentId: tournament.id,
      name: player.name,
      index: player.index,
      categoryKey: player.adjustmentCategory ?? "None"
    }))
  });

  const dbPlayers = await prisma.player.findMany({
    where: { tournamentId: tournament.id }
  });
  const playerByName = new Map(dbPlayers.map((player) => [player.name, player]));

  for (const courseSeed of seed.courses) {
    const course = await prisma.course.create({
      data: {
        tournamentId: tournament.id,
        name: courseSeed.name,
        type: courseSeed.type,
        tee: courseSeed.tee ?? null,
        yardage: courseSeed.yardage ?? null,
        slope: courseSeed.slope ?? null,
        rating: (courseSeed as any).rating ?? null,
        parTotal: courseSeed.par_total ?? null
      }
    });

    await prisma.hole.createMany({
      data: courseSeed.holes.map((hole) => ({
        courseId: course.id,
        holeNumber: hole.n,
        par: hole.par,
        strokeIndex: hole.si,
        segment: hole.n <= 9 ? "front" : "back"
      }))
    });
  }

  await prisma.sideContestDefinition.createMany({
    data: [
      { tournamentId: tournament.id, type: "LongDrive", amount: seed.tournament.payouts.sideContests["Long Drive"] ?? 0 },
      { tournamentId: tournament.id, type: "ClosestToPin", amount: seed.tournament.payouts.sideContests["Closest to Pin"] ?? 0 },
      { tournamentId: tournament.id, type: "LongPutt", amount: seed.tournament.payouts.sideContests["Long Putt"] ?? 0 },
      { tournamentId: tournament.id, type: "SecretSnowman", amount: seed.tournament.payouts.sideContests["Secret Snowman"] ?? 0 }
    ]
  });

  const dayByName = new Map<string, string>();
  for (const daySeed of seed.days) {
    const course = await prisma.course.findFirst({
      where: { tournamentId: tournament.id, name: daySeed.course }
    });
    if (!course) {
      throw new Error(`Course not found: ${daySeed.course}`);
    }

    const strokePlayRule =
      daySeed.strokePlayRule === "top8_fixed2"
        ? "strokePlayTop8Fixed"
        : daySeed.strokePlayRule === "fullScale"
          ? "strokePlayFull"
          : "none";

    const day = await prisma.day.create({
      data: {
        tournamentId: tournament.id,
        name: daySeed.name,
        courseId: course.id,
        isMatchPlay: daySeed.matchRounds.length > 0,
        strokePlayRule: strokePlayRule as any,
        isScramble: Boolean(daySeed.scramble)
      }
    });

    if (daySeed.matchRounds.length) {
      const schedule = seed.scheduleByCourse[daySeed.course] ?? {};
      const scheduleByLabel = schedule as Record<string, { a: string; b: string }[]>;
      for (const roundSeed of daySeed.matchRounds) {
        const matchRound = await prisma.matchRound.create({
          data: {
            dayId: day.id,
            segmentType: roundSeed.segment === "front9" ? "front" : "back",
            label: roundSeed.label,
            order: roundSeed.label.includes("#2") ? 3 : roundSeed.label.includes("Back") ? 2 : 1
          }
        });

        const pairings = scheduleByLabel[roundSeed.label] ?? [];
        const segmentKey = `${daySeed.name}|${roundSeed.segment}`;
        const eligiblePlayers = playersWithScores.get(segmentKey);
        for (const pairing of pairings) {
          const playerA = playerByName.get(pairing.a);
          const playerB = playerByName.get(pairing.b);
          if (!playerA || !playerB) {
            throw new Error(`Pairing players not found: ${pairing.a} vs ${pairing.b}`);
          }
          if (eligiblePlayers && (!eligiblePlayers.has(pairing.a) || !eligiblePlayers.has(pairing.b))) {
            continue;
          }
          await prisma.matchPairing.create({
            data: {
              matchRoundId: matchRound.id,
              playerAId: playerA.id,
              playerBId: playerB.id
            }
          });
        }
      }
    }

    const contestHoles = seed.sideContestHolesByCourse[daySeed.course];
    if (contestHoles) {
      await prisma.daySideContestConfig.createMany({
        data: [
          { dayId: day.id, type: "LongDrive", holeNumber: contestHoles.longDriveHole },
          { dayId: day.id, type: "ClosestToPin", holeNumber: contestHoles.ctpHole },
          { dayId: day.id, type: "LongPutt", holeNumber: contestHoles.longPuttHole },
          { dayId: day.id, type: "SecretSnowman", holeNumber: 0 }
        ]
      });
    }

    if (daySeed.scramble) {
      for (const teamSeed of daySeed.scramble.teams) {
        const team = await prisma.scrambleTeam.create({
          data: {
            dayId: day.id,
            name: `Team ${teamSeed.team}`
          }
        });
        await prisma.scrambleScore.create({
          data: {
            dayId: day.id,
            teamId: team.id,
            total: teamSeed.total
          }
        });
        for (const playerName of teamSeed.players) {
          const player = playerByName.get(playerName);
          if (!player) continue;
          await prisma.scrambleTeamPlayer.create({
            data: {
              teamId: team.id,
              playerId: player.id
            }
          });
        }
      }
    }

    dayByName.set(daySeed.name, day.id);
  }

  const cashByPlayer = seed.golden.cashByPlayer as Record<string, any>;
  const sideContestDays = await prisma.daySideContestConfig.findMany({
    include: { day: true }
  });

  const daysByContest = new Map<string, string[]>();
  sideContestDays.forEach((config) => {
    const key = config.type;
    const list = daysByContest.get(key) ?? [];
    list.push(config.dayId);
    daysByContest.set(key, list);
  });

  for (const [playerName, cashInfo] of Object.entries(cashByPlayer)) {
    const player = playerByName.get(playerName);
    if (!player || !cashInfo) {
      continue;
    }

    const sideWins = cashInfo.sideContestCounts as Record<string, number> | undefined;
    if (!sideWins) {
      continue;
    }

    for (const [typeName, count] of Object.entries(sideWins)) {
      const type =
        typeName === "Long Drive"
          ? "LongDrive"
          : typeName === "Closest to Pin"
            ? "ClosestToPin"
            : typeName === "Long Putt"
              ? "LongPutt"
              : typeName === "Secret Snowman"
                ? "SecretSnowman"
                : typeName;
      if (type === "SecretSnowman") {
        continue;
      }
      const dayIds = daysByContest.get(type) ?? [];
      if (!dayIds.length) {
        continue;
      }
      for (let i = 0; i < count; i += 1) {
        const dayId = dayIds[i % dayIds.length];
        await prisma.sideContestResult.create({
          data: {
            dayId,
            type: type as any,
            playerId: player.id,
            measurement: 0,
            isManualWinnerOverride: false
          }
        });
      }
    }
  }

  const secretDays = daysByContest.get("SecretSnowman") ?? [];
  if (secretDays.length) {
    const winners: string[] = [];
    for (const [playerName, cashInfo] of Object.entries(cashByPlayer)) {
      const wins = cashInfo?.sideContestCounts?.["Secret Snowman"] ?? 0;
      for (let i = 0; i < wins; i += 1) {
        const player = playerByName.get(playerName);
        if (player) winners.push(player.id);
      }
    }

    for (let i = 0; i < secretDays.length; i += 1) {
      const dayId = secretDays[i];
      const playerId = winners[i % winners.length] ?? dbPlayers[0]?.id;
      if (!playerId) continue;
      await prisma.secretSnowmanDraw.create({
        data: {
          dayId,
          playerId,
          seed: `seed-${i}`,
          drawnAt: new Date(),
          isLocked: true
        }
      });
    }
  }

  if (seedScores.length) {
    for (const score of seedScores) {
      const dayId = dayByName.get(score.day);
      const player = playerByName.get(score.player);
      if (!dayId || !player) continue;
      if (score.gross === null || score.gross === undefined) continue;
      const segment = score.segment === "front9" ? "front" : score.segment === "back9" ? "back" : score.segment;
      await prisma.daySegmentScore.upsert({
        where: {
          dayId_segmentType_playerId_holeNumber: {
            dayId,
            segmentType: segment as any,
            playerId: player.id,
            holeNumber: score.holeNumber
          }
        },
        update: { gross: score.gross },
        create: {
          dayId,
          segmentType: segment as any,
          playerId: player.id,
          holeNumber: score.holeNumber,
          gross: score.gross
        }
      });
    }
  }
}
