const { v4: uuidv4 } = require('uuid');
const { getJobRepository } = require('../models/Job');
const { defaultQueue } = require('../queue/jobQueue');
const { JOB_STATUS, JOB_PRIORITY, JOB_TYPES } = require('../queue/constants');
const config = require('../config');

class JobService {
  /**
   * Submit a new job into the queue & database
   */
  async submitJob({
    type,
    payload = {},
    priority = JOB_PRIORITY.MEDIUM,
    maxRetries = config.worker.defaultMaxRetries,
    backoffMs = config.worker.defaultRetryBackoffMs,
  }) {
    if (!type || !Object.values(JOB_TYPES).includes(type)) {
      throw new Error(`Invalid job type: ${type}. Allowed: ${Object.values(JOB_TYPES).join(', ')}`);
    }

    const validPriority = Object.values(JOB_PRIORITY).includes(priority)
      ? priority
      : JOB_PRIORITY.MEDIUM;

    const jobId = `job_${uuidv4()}`;
    const JobRepo = getJobRepository();

    const jobDoc = await JobRepo.create({
      jobId,
      type,
      payload,
      priority: validPriority,
      status: JOB_STATUS.PENDING,
      maxRetries,
      backoffMs,
      logs: [
        {
          timestamp: new Date(),
          message: `Job enqueued with priority ${validPriority}`,
          status: JOB_STATUS.PENDING,
        },
      ],
    });

    // Enqueue in Redis
    await defaultQueue.enqueue(jobId, validPriority);

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

    return await JobRepo.find(filter, { limit: parseInt(limit, 10), skip: parseInt(skip, 10) });
  }

  /**
   * Manually retry a failed job
   */
  async retryJob(jobId) {
    const JobRepo = getJobRepository();
    const job = await JobRepo.findOne({ jobId });
    if (!job) throw new Error(`Job not found: ${jobId}`);

    if (job.status !== JOB_STATUS.FAILED) {
      throw new Error(`Only FAILED jobs can be manually retried. Current status: ${job.status}`);
    }

    await JobRepo.updateOne(
      { jobId },
      {
        $set: { status: JOB_STATUS.PENDING, error: null },
        $push: {
          logs: {
            timestamp: new Date(),
            message: 'Job manually re-queued for retry',
            status: JOB_STATUS.PENDING,
          },
        },
      }
    );

    await defaultQueue.enqueue(jobId, job.priority);
    return await JobRepo.findOne({ jobId });
  }

  /**
   * Clear all jobs and queue contents (utility for reset/benchmark)
   */
  async clearAll() {
    const JobRepo = getJobRepository();
    await JobRepo.deleteMany({});
    await defaultQueue.clearAll();
    return { success: true, message: 'All queues and job records cleared' };
  }
}

module.exports = new JobService();
