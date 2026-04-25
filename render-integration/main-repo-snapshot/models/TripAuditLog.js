const mongoose = require('mongoose');

const TripAuditLogSchema = new mongoose.Schema(
  {
    tripId: { type: mongoose.Schema.Types.ObjectId, ref: 'Trip', required: true, index: true },
    action: { type: String, required: true, trim: true },
    actor: { type: String, enum: ['admin', 'public'], default: 'public' },
    method: { type: String, default: '' },
    route: { type: String, default: '' },
    summary: { type: String, default: '' },
    details: { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
    timestamp: { type: Date, default: Date.now, index: true },
  },
  { timestamps: false }
);

TripAuditLogSchema.index({ tripId: 1, timestamp: -1 });

const TripAuditLogModel = mongoose.model('TripAuditLog', TripAuditLogSchema);

module.exports = TripAuditLogModel;
module.exports.schema = TripAuditLogSchema;
