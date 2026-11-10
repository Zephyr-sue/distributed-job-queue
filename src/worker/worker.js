const os = require('os');
const { connectDB } = require('../database/mongo');
const { getJobRepository } = require('../models/Job');
const { JobQueue } = require('../queue/jobQueue');
const { JOB_STATUS } = require('../queue/constants');
const { getHandler } = require('../handlers');
const config = require('../config');

class WorkerProcess {
  constructor(workerId, options = {}) {
    this.workerId = workerId || `worker-${os.hostname()}-${process.pid}-${Math.random().toString(36).substring(2, 6)}`;
    this.isRunning = false;
    this.isProcessing = false;
    this.currentJobId = null;
    this.queue = options.queue || new JobQueue(options);
    this.heartbeatTimer = null;
  }

  async start() {
    console.log(`🚀 [${this.workerId}] Starting worker process...`);
    await connectDB();
    await this.queue.init(this.workerId);
    this.isRunning = true;

    // Start worker heartbeat & registration
    await this.queue.recordHeartbeat(this.workerId, config.worker.heartbeatTtlSec);
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.queue.recordHeartbeat(this.workerId, config.worker.heartbeatTtlSec);
      } catch (err) {
        // Heartbeat error
      }
    }, config.worker.heartbeatIntervalMs);

    if (this.heartbeatTimer.unref) {
      this.heartbeatTimer.unref();
    }

    // Start background orphan reaper daemon
    this.queue.startOrphanReaper(config.worker.orphanReaperIntervalMs);

    console.log(`✅ [${this.workerId}] Listening for jobs across HIGH, MEDIUM, LOW queues (Heartbeat: ${config.worker.heartbeatIntervalMs}ms)`);

    this.registerSignalHandlers();
    this.runLoop();
  }

  registerSignalHandlers() {
    const shutdown = async (signal) => {
      console.log(`\n🛑 [${this.workerId}] Received ${signal}. Initiating graceful shutdown...`);
      this.isRunning = false;

      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
      }

      const checkInterval = setInterval(async () => {
        if (!this.isProcessing) {
          clearInterval(checkInterval);
          await this.queue.unregisterWorker(this.workerId);
          await this.queue.close();
          console.log(`👋 [${this.workerId}] Worker shutdown cleanly.`);
          process.exit(0);
        } else {
          console.log(`⏳ [${this.workerId}] Waiting for active job ${this.currentJobId || ''} to finish before exiting...`);
        }
      }, 500);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  async runLoop() {
    while (this.isRunning) {
      try {
        const item = await this.queue.fetchNext(this.workerId, config.worker.pollTimeoutSec);
        if (!item) {
          continue;
        }

        this.isProcessing = true;
        this.currentJobId = item.jobId;
        await this.processJob(item.jobId, item.priority);
      } catch (err) {
        console.error(`❌ [${this.workerId}] Worker loop error:`, err.message);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } finally {
        this.isProcessing = false;
        this.currentJobId = null;
      }
    }
  }

  async processJob(jobId, queuePriority) {
    const startTime = Date.now();
    const JobRepo = getJobRepository();
    const job = await JobRepo.findOne({ jobId });

    if (!job) {
      console.warn(`⚠️ [${this.workerId}] Job ID ${jobId} not found in DB. Acknowledging & skipping.`);
      await this.queue.ackJob(jobId, this.workerId);
      return;
    }

    // Distributed Lock / Idempotency Guard: prevent duplicate concurrent executions
    const lockAcquired = await this.queue.acquireJobLock(jobId, this.workerId, config.worker.jobLockTtlSec);
    if (!lockAcquired) {
      console.warn(`🔒 [${this.workerId}] Idempotency guard: Job ${jobId} is already locked by another worker. Skipping duplicate execution.`);
      return;
    }

    const currentAttempt = (job.attempts || 0) + 1;
    console.log(`⚡ [${this.workerId}] Processing ${job.type} (ID: ${jobId}, Priority: ${queuePriority}, Attempt: ${currentAttempt}/${job.maxRetries || 3})`);

    // Mark as PROCESSING in MongoDB
    await JobRepo.updateOne(
      { jobId },
      {
        $set: {
          status: JOB_STATUS.PROCESSING,
          workerId: this.workerId,
          processedAt: new Date(),
        },
        $inc: { attempts: 1 },
        $push: {
          logs: {
            timestamp: new Date(),
            message: `Execution started on ${this.workerId} (Attempt #${currentAttempt})`,
            status: JOB_STATUS.PROCESSING,
            workerId: this.workerId,
          },
        },
      }
    );

    try {
      const handler = getHandler(job.type);
      const jobData = typeof job.toObject === 'function' ? job.toObject() : job;
      const result = await handler(job.payload || {}, { ...jobData, attempts: currentAttempt });
      const durationMs = Date.now() - startTime;

      // Mark COMPLETED in MongoDB
      await JobRepo.updateOne(
        { jobId },
        {
          $set: {
            status: JOB_STATUS.COMPLETED,
            result,
            error: null,
            executionTimeMs: durationMs,
            completedAt: new Date(),
          },
          $push: {
            logs: {
              timestamp: new Date(),
              message: `Job completed successfully in ${durationMs}ms`,
              status: JOB_STATUS.COMPLETED,
              workerId: this.workerId,
            },
          },
        }
      );

      await this.queue.ackJob(jobId, this.workerId);
      await this.queue.releaseJobLock(jobId);
      console.log(`✅ [${this.workerId}] Completed ${job.type} (${jobId}) in ${durationMs}ms`);
    } catch (error) {
      const durationMs = Date.now() - startTime;
      const maxRetries = job.maxRetries || config.worker.defaultMaxRetries;
      const baseBackoff = job.backoffMs || config.worker.defaultRetryBackoffMs;

      if (currentAttempt < maxRetries) {
        // Multi-stage retry with Exponential Backoff + Full Jitter (AWS recommended pattern to prevent thundering herds)
        const maxBackoff = baseBackoff * Math.pow(2, currentAttempt - 1);
        const backoffDelay = Math.floor(Math.random() * maxBackoff) + 100;

        console.warn(`⚠️ [${this.workerId}] Job ${jobId} failed (${error.message}). Scheduling retry in ${backoffDelay}ms (with jitter) (Attempt ${currentAttempt}/${maxRetries})`);

        await JobRepo.updateOne(
          { jobId },
          {
            $set: {
              status: JOB_STATUS.RETRYING,
              error: error.message,
            },
            $push: {
              logs: {
                timestamp: new Date(),
                message: `Attempt #${currentAttempt} failed: ${error.message}. Retrying in ${backoffDelay}ms (exponential backoff + jitter)`,
                status: JOB_STATUS.RETRYING,
                workerId: this.workerId,
              },
            },
          }
        );

        // Schedule delayed retry in Redis ZSET and release lock for future attempt
        await this.queue.scheduleRetry(jobId, job.priority, backoffDelay, this.workerId);
        await this.queue.releaseJobLock(jobId);
      } else {
        // Max retries exhausted -> Move to Dead Letter Queue (DLQ)
        console.error(`💥 [${this.workerId}] Job ${jobId} permanently FAILED after ${currentAttempt} attempts. Routing to Dead Letter Queue (DLQ). Error: ${error.message}`);

        await JobRepo.updateOne(
          { jobId },
          {
            $set: {
              status: JOB_STATUS.DLQ,
              error: error.message,
              failedAt: new Date(),
              executionTimeMs: durationMs,
            },
            $push: {
              logs: {
                timestamp: new Date(),
                message: `Job retries exhausted (${currentAttempt} attempts): ${error.message}. Routed to Dead Letter Queue (DLQ).`,
                status: JOB_STATUS.DLQ,
                workerId: this.workerId,
              },
            },
          }
        );

        await this.queue.moveToDLQ(jobId, this.workerId);
        await this.queue.releaseJobLock(jobId);
      }
    }
  }
}

if (require.main === module) {
  const worker = new WorkerProcess();
  worker.start().catch((err) => {
    console.error('Fatal worker error:', err);
    process.exit(1);
  });
}

module.exports = { WorkerProcess };
