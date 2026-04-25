const mongoose = require('mongoose');
const TripParticipantSchema = new mongoose.Schema({
  trip: { type: mongoose.Schema.Types.ObjectId, ref: 'Trip', required: true },
  name: { type: String, required: true },
  status: { type: String, enum: ['invited', 'in', 'maybe', 'out', 'waitlist'], default: 'in' },
  handicapIndex: { type: Number, default: null },
  depositPaidAmount: { type: Number, default: 0 },
  totalPaidAmount: { type: Number, default: 0 },
  depositPaidDate: { type: Date },
  wantsExtraNight: { type: Boolean, default: false },
  roomAssignment: { type: String },
  email: { type: String },
  phone: { type: String },
  notes: { type: String },
}, { timestamps: true });
const TripParticipantModel = mongoose.model('TripParticipant', TripParticipantSchema);
module.exports = TripParticipantModel;
module.exports.schema = TripParticipantSchema;
