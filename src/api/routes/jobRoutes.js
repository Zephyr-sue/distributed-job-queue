const express = require('express');
const router = express.Router();
const jobService = require('../../services/jobService');

/**
 * POST /api/jobs - Submit a new job with idempotency & backpressure support
 */
router.post('/', async (req, res) => {
  try {
    const { type, payload, priority, maxRetries, backoffMs } = req.body;
    // Idempotency key can come from HTTP header or request body
    const idempotencyKey = req.headers['idempotency-key'] || req.body.idempotencyKey;

    if (!type) {
      return res.status(400).json({ error: 'Missing required field: "type"' });
    }

    const job = await jobService.submitJob({
      type,
      payload,
      priority,
      maxRetries,
      backoffMs,
      idempotencyKey,
    });

    const statusCode = job.isIdempotentDuplicate ? 200 : 202;

    return res.status(statusCode).json({
      message: job.isIdempotentDuplicate
        ? 'Job recognized via Idempotency-Key (deduplicated)'
        : 'Job submitted successfully',
      jobId: job.jobId,
      status: job.status,
      priority: job.priority,
      type: job.type,
      isIdempotentDuplicate: !!job.isIdempotentDuplicate,
      createdAt: job.createdAt,
    });
  } catch (err) {
    if (err.isBackpressure) {
      res.setHeader('Retry-After', '5');
      return res.status(429).json({
        error: 'Too Many Requests',
        message: err.message,
        currentDepth: err.currentDepth,
        maxDepth: err.maxDepth,
        retryAfterSeconds: 5,
      });
    }
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/jobs/bulk - Batch submit multiple jobs
 */
router.post('/bulk', async (req, res) => {
  try {
    const { jobs } = req.body;
    if (!Array.isArray(jobs) || jobs.length === 0) {
      return res.status(400).json({ error: 'Body must contain an array of jobs: { jobs: [...] }' });
    }

    const results = await jobService.submitBatch(jobs);
    return res.status(202).json({
      message: `Enqueued ${results.length} jobs`,
      count: results.length,
      jobs: results.map((j) => ({ jobId: j.jobId, type: j.type, priority: j.priority })),
    });
  } catch (err) {
    if (err.isBackpressure) {
      res.setHeader('Retry-After', '5');
      return res.status(429).json({
        error: 'Too Many Requests',
        message: err.message,
        currentDepth: err.currentDepth,
        maxDepth: err.maxDepth,
        retryAfterSeconds: 5,
      });
    }
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/jobs - Query recent jobs with filters
 */
router.get('/', async (req, res) => {
  try {
    const { status, priority, type, limit = 50, skip = 0 } = req.query;
    const jobs = await jobService.listJobs({ status, priority, type, limit, skip });
    return res.json({ count: jobs.length, jobs });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/jobs/dlq/replay - Replay jobs from Dead Letter Queue into active queue
 */
router.post('/dlq/replay', async (req, res) => {
  try {
    const limit = parseInt(req.body.limit, 10) || 50;
    const result = await jobService.replayDeadLetterQueue(limit);
    return res.json({
      message: `Successfully replayed ${result.replayedCount} DLQ jobs back into active processing`,
      replayedCount: result.replayedCount,
      jobIds: result.jobIds,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/jobs/reap-orphans - Manually trigger orphan reaper
 */
router.post('/reap-orphans', async (req, res) => {
  try {
    const reaped = await jobService.reclaimOrphanedJobs();
    return res.json({
      message: `Reaped ${reaped.length} orphaned jobs from dead workers`,
      reaped,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/jobs/:id - Get specific job detail and execution audit log
 */
router.get('/:id', async (req, res) => {
  try {
    const job = await jobService.getJobById(req.params.id);
    if (!job) {
      return res.status(404).json({ error: `Job with ID "${req.params.id}" not found` });
    }
    return res.json(job);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/jobs/:id/retry - Manually trigger retry for failed or DLQ job
 */
router.post('/:id/retry', async (req, res) => {
  try {
    const job = await jobService.retryJob(req.params.id);
    return res.json({ message: 'Job retry scheduled', job });
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }
});

module.exports = router;
