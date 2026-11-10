const { v4: uuidv4 } = require('uuid');
const { getJobRepository } = require('../models/Job');
const { defaultQueue } = require('../queue/jobQueue');
const { JOB_STATUS, JOB_PRIORITY, JOB_TYPES } = require('../queue/constants');
const config = require('../config');

class BackpressureError extends Error {
  constructor(message, currentDepth, maxDepth) {
    super(message);
    this.name = 'BackpressureError';
    this.isBackpressure = true;
    this.currentDepth = currentDepth;
    this.maxDepth = maxDepth;
  }
}

class JobService {
  constructor(queue = null) {
    this.queue = queue;
  }

  getQueue() {
    return this.queue || defaultQueue;
  }

  setQueue(queue) {
    this.queue = queue;
  }

  /**
   * Submit a new job into the queue & database with backpressure & idempotency checks
   */
  async submitJob({
    type,
    payload = {},
    priority = JOB_PRIORITY.MEDIUM,
    maxRetries = config.worker.defaultMaxRetries,
    backoffMs = config.worker.defaultRetryBackoffMs,
    idempotencyKey = null,
  }) {
    if (!type || !Object.values(JOB_TYPES).includes(type)) {
      throw new Error(`Invalid job type: ${type}. Allowed: ${Object.values(JOB_TYPES).join(', ')}`);
    }

    const validPriority = Object.values(JOB_PRIORITY).includes(priority)
      ? priority
      : JOB_PRIORITY.MEDIUM;

    const JobRepo = getJobRepository();
    const activeQueue = this.getQueue();

    // 1. Check producer backpressure: reject or throttle if Redis queue is saturated
    const metrics = await activeQueue.getMetrics();
    if (metrics.totalPending >= config.queue.maxQueueDepth) {
      throw new BackpressureError(
        `Queue backpressure limit exceeded: current pending (${metrics.totalPending}) >= max capacity (${config.queue.maxQueueDepth})`,
        metrics.totalPending,
        config.queue.maxQueueDepth
      );
    }

    const jobId = `job_${uuidv4()}`;

    // 2. Client-provided Idempotency Key deduplication
    if (idempotencyKey) {
      const idempotencyResult = await activeQueue.checkOrSetIdempotencyKey(idempotencyKey, jobId);

      if (!idempotencyResult.isNew) {
        // Return previously submitted job
        const existingJob = await JobRepo.findOne({ jobId: idempotencyResult.jobId });
        if (existingJob) {
          const jobObj = typeof existingJob.toObject === 'function' ? existingJob.toObject() : existingJob;
          return { ...jobObj, isIdempotentDuplicate: true };
        }
      }
    }

    const jobDoc = await JobRepo.create({
      jobId,
      type,
      payload,
      priority: validPriority,
      status: JOB_STATUS.PENDING,
      maxRetries,
      backoffMs,
      idempotencyKey: idempotencyKey || null,
      logs: [
        {
          timestamp: new Date(),
          message: `Job enqueued with priority ${validPriority}${idempotencyKey ? ` (IdempotencyKey: ${idempotencyKey})` : ''}`,
          status: JOB_STATUS.PENDING,
        },
      ],
    });

    // Enqueue in Redis
    await activeQueue.enqueue(jobId, validPriority);

    return jobDoc;
  }

  /**
   * Bulk submit jobs (useful for load testing & benchmarking)
   */
  async submitBatch(jobsArray) {
    const results = [];
    for (const item of jobsArray) {
      const job = await this.submitJob(item);
      results.push(job);
    }
    return results;
  }

  /**
   * Get single job by ID
   */
  async getJobById(jobId) {
    const JobRepo = getJobRepository();
    return await JobRepo.findOne({ jobId });
  }

  /**
   * List jobs with filters and pagination
   */
  async listJobs({ status, priority, type, limit = 50, skip = 0 }) {
    const JobRepo = getJobRepository();
    const filter = {};
    if (status) filter.status = status;
    if (priority) filter.priority = priority;
    if (type) filter.type = type;

    return await JobRepo.find(filter)
      .sort({ createdAt: -1 })
      .skip(parseInt(skip, 10) || 0)
      .limit(parseInt(limit, 10) || 50)
      .lean();
  }

  /**
   * Manually retry a failed or DLQ job
   */
  async retryJob(jobId) {
    const JobRepo = getJobRepository();
    const job = await JobRepo.findOne({ jobId });
    if (!job) throw new Error(`Job not found: ${jobId}`);

    if (job.status !== JOB_STATUS.FAILED && job.status !== JOB_STATUS.DLQ) {
      throw new Error(`Only FAILED or DLQ jobs can be manually retried. Current status: ${job.status}`);
    }

    await JobRepo.updateOne(
      { jobId },
      {
        $set: { status: JOB_STATUS.PENDING, error: null },
        $push: {
          logs: {
            timestamp: new Date(),
            message: 'Manual retry initiated by operator',
            status: JOB_STATUS.PENDING,
          },
        },
      }
    );

    await this.getQueue().enqueue(jobId, job.priority || JOB_PRIORITY.MEDIUM);
    return { jobId, status: JOB_STATUS.PENDING };
  }

  /**
   * Replay jobs from Dead Letter Queue
   */
  async replayDeadLetterQueue(limit = 100) {
    const JobRepo = getJobRepository();
    const replayedJobIds = await this.getQueue().replayDLQ(limit);

    for (const jobId of replayedJobIds) {
      await JobRepo.updateOne(
        { jobId },
        {
          $set: { status: JOB_STATUS.PENDING, error: null },
          $push: {
            logs: {
              timestamp: new Date(),
              message: 'Job replayed from DLQ into active queue',
              status: JOB_STATUS.PENDING,
            },
          },
        }
      );
    }

    return { replayedCount: replayedJobIds.length, jobIds: replayedJobIds };
  }

  /**
   * Trigger orphan reclamation manually or via health check
   */
  async reclaimOrphanedJobs() {
    return await this.getQueue().reapOrphanedJobs();
  }

  /**
   * Clear all queues & jobs
   */
  async clearAll() {
    const JobRepo = getJobRepository();
    await JobRepo.deleteMany({});
    await this.getQueue().clearAll();
    return { message: 'All queues and database records cleared' };
  }
}

module.exports = new JobService();
module.exports.BackpressureError = BackpressureError;
module.exports.JobService = JobService;
