require('dotenv').config();
const mongoose = require('mongoose');
const Trip = require('../models/Trip');
const TripParticipant = require('../models/TripParticipant');

const TIN_CUP_PLAYERS = [
  { name: 'Matt', handicapIndex: 10.8 },
  { name: 'Rick', handicapIndex: 15.3 },
  { name: 'OB', handicapIndex: 11.2 },
  { name: 'Kyle', handicapIndex: 8.2 },
  { name: 'Manny', handicapIndex: 22.0 },
  { name: 'Steve', handicapIndex: 13.1 },
  { name: 'Tommy', handicapIndex: 9.1 },
  { name: 'Pat', handicapIndex: 17.6 },
  { name: 'Mil', handicapIndex: 12.9 },
  { name: 'Paul O', handicapIndex: 15.0 },
  { name: 'Brian', handicapIndex: 20.9 },
  { name: 'Bob', handicapIndex: 22.0 },
  { name: 'David', handicapIndex: 12.9 },
  { name: 'John', handicapIndex: 11.2 },
  { name: 'Tony', handicapIndex: 12.8 },
  { name: 'Spiro', handicapIndex: 24.5 }
];

const TIN_CUP_TRIP = {
  name: 'Tin Cup 2026 Trip',
  groupName: 'Tin Cup 5/12-5/17/26',
  location: 'Southern Pines, NC',
  arrivalDate: new Date('2026-05-12T00:00:00.000Z'),
  departureDate: new Date('2026-05-17T00:00:00.000Z'),
  packageType: '5 Nights / 5 Rounds',
  reservationNumber: 'Tin Cup 2026',
  preparedBy: 'Tin Cup Trip Planner',
  baseGroupSize: 16
};

function registerModelsForConnection(conn) {
  const TripModel = conn.models.Trip || conn.model('Trip', Trip.schema);
  const TripParticipantModel = conn.models.TripParticipant || conn.model('TripParticipant', TripParticipant.schema);
  return { TripModel, TripParticipantModel };
}

async function seedForConnection(conn, label) {
  const { TripModel, TripParticipantModel } = registerModelsForConnection(conn);
  const existingTrips = await TripModel.find({ name: /tin\s*cup/i }).sort({ createdAt: 1 });
  const trip = existingTrips.length
    ? await TripModel.findByIdAndUpdate(existingTrips[0]._id, { $set: TIN_CUP_TRIP }, { new: true })
    : await TripModel.create(TIN_CUP_TRIP);

  if (existingTrips.length > 1) {
    const duplicateIds = existingTrips.slice(1).map((doc) => doc._id);
    if (duplicateIds.length) {
      await TripParticipantModel.deleteMany({ trip: { $in: duplicateIds } });
      await TripModel.deleteMany({ _id: { $in: duplicateIds } });
    }
  }

  const existingParticipants = await TripParticipantModel.find({ trip: trip._id });
  const byName = new Map(existingParticipants.map((p) => [String(p.name).trim().toLowerCase(), p]));
  const targetNames = new Set(TIN_CUP_PLAYERS.map((p) => p.name.trim().toLowerCase()));
  let created = 0;
  let updated = 0;
  let removed = 0;

  for (const player of TIN_CUP_PLAYERS) {
    const key = player.name.trim().toLowerCase();
    const existing = byName.get(key);
    if (existing) {
      await TripParticipantModel.updateOne(
        { _id: existing._id },
        { $set: { name: player.name, handicapIndex: player.handicapIndex, status: 'in' } }
      );
      updated += 1;
    } else {
      await TripParticipantModel.create({
        trip: trip._id,
        name: player.name,
        handicapIndex: player.handicapIndex,
        status: 'in'
      });
      created += 1;
    }
  }

  for (const participant of existingParticipants) {
    const key = String(participant.name || '').trim().toLowerCase();
    if (!targetNames.has(key)) {
      await TripParticipantModel.deleteOne({ _id: participant._id });
      removed += 1;
    }
  }

  const count = await TripParticipantModel.countDocuments({ trip: trip._id });
  console.log(`${label}: trip=${trip._id} participants=${count} created=${created} updated=${updated} removed=${removed}`);
  return { tripId: String(trip._id), participantCount: count };
}

async function main() {
  const primaryUri = String(process.env.MONGO_URI || '').trim();
  const secondaryUri = String(process.env.MONGO_URI_SECONDARY || '').trim();
  if (!primaryUri) throw new Error('MONGO_URI is not configured');

  const primary = await mongoose.createConnection(primaryUri, {
    dbName: process.env.MONGO_DB || undefined
  }).asPromise();

  let secondary = null;
  try {
    const primaryResult = await seedForConnection(primary, 'primary');
    let secondaryResult = null;
    if (secondaryUri) {
      secondary = await mongoose.createConnection(secondaryUri, {
        dbName: process.env.MONGO_DB_SECONDARY || undefined
      }).asPromise();
      secondaryResult = await seedForConnection(secondary, 'secondary');
    } else {
      console.log('secondary: skipped (MONGO_URI_SECONDARY not configured)');
    }
    console.log(JSON.stringify({ ok: true, primary: primaryResult, secondary: secondaryResult }, null, 2));
  } finally {
    await primary.close().catch(() => {});
    if (secondary) await secondary.close().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
