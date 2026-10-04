const mongoose = require('mongoose');
const { JOB_STATUS, JOB_PRIORITY, JOB_TYPES } = require('../queue/constants');
const { isFallbackMode } = require('../database/mongo');

const jobSchema = new mongoose.Schema(
  {
    jobId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    type: {
      type: String,
      enum: Object.values(JOB_TYPES),
      required: true,
      index: true,
    },
    payload: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    priority: {
      type: String,
      enum: Object.values(JOB_PRIORITY),
      default: JOB_PRIORITY.MEDIUM,
      index: true,
    },
    status: {
      type: String,
      enum: Object.values(JOB_STATUS),
      default: JOB_STATUS.PENDING,
      index: true,
    },
    attempts: {
      type: Number,
      default: 0,
    },
    maxRetries: {
      type: Number,
      default: 3,
    },
    backoffMs: {
      type: Number,
      default: 2000,
    },
    workerId: {
      type: String,
      default: null,
    },
    result: {
      type: mongoose.Schema.Types.Mixed,
      default: null,
    },
    error: {
      type: String,
      default: null,
    },
    logs: [
      {
        timestamp: { type: Date, default: Date.now },
        message: String,
        status: String,
        workerId: String,
      },
    ],
    executionTimeMs: {
      type: Number,
      default: 0,
    },
    processedAt: {
      type: Date,
      default: null,
    },
    completedAt: {
      type: Date,
      default: null,
    },
    failedAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

const MongoJobModel = mongoose.model('Job', jobSchema);

// In-memory document collection when running in fallback mode
class InMemoryJobRepository {
  constructor() {
    this.jobs = new Map();
  }

  async create(data) {
    const now = new Date();
    const doc = {
      _id: 'mem_' + Math.random().toString(36).substring(2, 9),
      ...data,
      attempts: data.attempts || 0,
      maxRetries: data.maxRetries || 3,
      backoffMs: data.backoffMs || 2000,
      logs: data.logs || [{ timestamp: now, message: 'Job created', status: data.status || JOB_STATUS.PENDING }],
      createdAt: now,
      updatedAt: now,
    };
    this.jobs.set(doc.jobId, doc);
    return JSON.parse(JSON.stringify(doc));
  }

  async findOne({ jobId }) {
    const job = this.jobs.get(jobId);
    return job ? JSON.parse(JSON.stringify(job)) : null;
  }

  async updateOne({ jobId }, updateQuery) {
    const job = this.jobs.get(jobId);
    if (!job) return { matchedCount: 0, modifiedCount: 0 };

    const $set = updateQuery.$set || {};
    const $push = updateQuery.$push || {};
    const $inc = updateQuery.$inc || {};

    Object.assign(job, $set);

    for (const [key, incVal] of Object.entries($inc)) {
      job[key] = (job[key] || 0) + incVal;
    }

    if ($push.logs) {
      if (!job.logs) job.logs = [];
      job.logs.push($push.logs);
    }

    job.updatedAt = new Date();
    this.jobs.set(jobId, job);
    return { matchedCount: 1, modifiedCount: 1 };
  }

  async find(filter = {}, options = {}) {
    let list = Array.from(this.jobs.values());
    if (filter.status) {
      list = list.filter((j) => j.status === filter.status);
    }
    if (filter.priority) {
      list = list.filter((j) => j.priority === filter.priority);
    }
    if (filter.type) {
      list = list.filter((j) => j.type === filter.type);
    }

    // Sort descending by createdAt
    list.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    const skip = options.skip || 0;
    const limit = options.limit || list.length;
    return list.slice(skip, skip + limit).map((j) => JSON.parse(JSON.stringify(j)));
  }

  async countDocuments(filter = {}) {
    let list = Array.from(this.jobs.values());
    if (filter.status) {
      list = list.filter((j) => j.status === filter.status);
    }
    return list.length;
  }

  async deleteMany(filter = {}) {
    let count = 0;
    if (Object.keys(filter).length === 0) {
      count = this.jobs.size;
      this.jobs.clear();
    } else if (filter.status) {
      for (const [id, job] of this.jobs.entries()) {
        if (job.status === filter.status) {
          this.jobs.delete(id);
          count++;
        }
      }
    }
    return { deletedCount: count };
  }
}

const memoryRepo = new InMemoryJobRepository();

module.exports = {
  MongoJobModel,
  memoryRepo,
  getJobRepository: () => {
    return isFallbackMode() ? memoryRepo : MongoJobModel;
  },
};
