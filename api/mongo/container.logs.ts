import mongoose from 'mongoose'

const containerLogSchema = new mongoose.Schema({
  eventId: {
    type: String,
    required: true,
    unique: true,
  },
  projectId: {
    type: String,
    required: true,
  },
  deploymentId: {
    type: String,
    required: true,
  },
  log: {
    type: String,
    required: true,
  },
  timestamp: {
    type: Date,
    required: true,
  }
})

containerLogSchema.index({ deploymentId: 1, timestamp: 1 })
containerLogSchema.index({ timestamp: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 30 })

export const containerLogs = mongoose.model('container-logs', containerLogSchema, 'container-logs')