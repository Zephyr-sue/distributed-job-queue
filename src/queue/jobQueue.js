const { QUEUE_KEYS, JOB_PRIORITY } = require('./constants');
const { createRedisClient } = require('./redis');
const config = require('../config');

class JobQueue {
  constructor(options = {}) {
    this.keyPrefix = typeof options === 'string' ? options : (options.keyPrefix || '');
    this.client = null;
    this.blockingClient = null;
    this.isInitialized = false;
    this.promoterTimer = null;
    this.reaperTimer = null;
  }

  _k(key) {
    return this.keyPrefix ? `${this.keyPrefix}:${key}` : key;
  }

  async init(label = 'main') {
    if (this.isInitialized) return;
    this.client = await createRedisClient(`${label}-client`);
    this.blockingClient = await createRedisClient(`${label}-blocking`);
    this.isInitialized = true;
    this.startDelayedJobPromoter();
  }

  getQueueKeyByPriority(priority) {
    let key;
    switch (priority) {
      case JOB_PRIORITY.HIGH:
        key = QUEUE_KEYS.HIGH;
        break;
      case JOB_PRIORITY.LOW:
        key = QUEUE_KEYS.LOW;
        break;
      case JOB_PRIORITY.MEDIUM:
      default:
        key = QUEUE_KEYS.MEDIUM;
        break;
    }
    return this._k(key);
  }

  /**
   * Enqueue a job ID into the appropriate priority queue (LPUSH)
   */
  async enqueue(jobId, priority = JOB_PRIORITY.MEDIUM) {
    if (!this.isInitialized) await this.init();
    const queueKey = this.getQueueKeyByPriority(priority);
    await this.client.lpush(queueKey, jobId);
    return { queueKey, jobId, priority };
  }

  /**
   * Atomic move helper (BLMOVE with fallback to BRPOPLPUSH or LMOVE)
   */
  async _atomicMove(source, destination, timeoutSec = 0) {
    try {
      if (typeof this.blockingClient.blmove === 'function' && timeoutSec > 0) {
        return await this.blockingClient.blmove(source, destination, 'RIGHT', 'LEFT', timeoutSec);
      }
      if (typeof this.client.lmove === 'function' && timeoutSec === 0) {
        return await this.client.lmove(source, destination, 'RIGHT', 'LEFT');
      }
    } catch (err) {
      // Fallback for older Redis or clients without BLMOVE
    }

    if (timeoutSec > 0 && typeof this.blockingClient.brpoplpush === 'function') {
      return await this.blockingClient.brpoplpush(source, destination, timeoutSec);
    }
    if (typeof this.client.rpoplpush === 'function') {
      return await this.client.rpoplpush(source, destination);
    }

    return null;
  }

  /**
   * Atomically fetch the highest priority available job using BLMOVE/RPOPLPUSH.
   * Jobs are moved into worker-specific in-flight processing queue.
   */
  async fetchNext(workerId = 'default-worker', timeoutSec = 2) {
    if (!this.isInitialized) await this.init();

    const processingQueue = this._k(QUEUE_KEYS.workerProcessingQueue(workerId));
    const highKey = this._k(QUEUE_KEYS.HIGH);
    const medKey = this._k(QUEUE_KEYS.MEDIUM);
    const lowKey = this._k(QUEUE_KEYS.LOW);

    // 1. Try non-blocking atomic move in priority order: HIGH -> MEDIUM -> LOW
    let jobId = await this._atomicMove(highKey, processingQueue, 0);
    let priority = JOB_PRIORITY.HIGH;

    if (!jobId) {
      jobId = await this._atomicMove(medKey, processingQueue, 0);
      priority = JOB_PRIORITY.MEDIUM;
    }

    if (!jobId) {
      jobId = await this._atomicMove(lowKey, processingQueue, 0);
      priority = JOB_PRIORITY.LOW;
    }

    // 2. If nothing ready, block for up to timeoutSec on HIGH queue
    if (!jobId && timeoutSec > 0) {
      jobId = await this._atomicMove(highKey, processingQueue, timeoutSec);
      priority = JOB_PRIORITY.HIGH;
    }

    if (!jobId) return null;

    // Track in global processing set for monitoring
    await this.client.sadd(this._k(QUEUE_KEYS.PROCESSING), jobId);

    return { jobId, priority, workerProcessingQueue: processingQueue };
  }

  /**
   * Schedule a job for delayed retry using Redis Sorted Set (ZSET)
   */
  async scheduleRetry(jobId, priority, delayMs, workerId = null) {
    if (!this.isInitialized) await this.init();
    const runAt = Date.now() + delayMs;
    const member = JSON.stringify({ jobId, priority });

    await this.client.zadd(this._k(QUEUE_KEYS.DELAYED), runAt, member);
    await this.client.srem(this._k(QUEUE_KEYS.PROCESSING), jobId);

    if (workerId) {
      await this.client.lrem(this._k(QUEUE_KEYS.workerProcessingQueue(workerId)), 0, jobId);
    }

    return { jobId, runAt };
  }

  /**
   * Move a permanently failed job into Dead Letter Queue (DLQ)
   */
  async moveToDLQ(jobId, workerId = null) {
    if (!this.isInitialized) await this.init();

    await this.client.lpush(this._k(QUEUE_KEYS.DLQ), jobId);
    await this.client.srem(this._k(QUEUE_KEYS.PROCESSING), jobId);

    if (workerId) {
      await this.client.lrem(this._k(QUEUE_KEYS.workerProcessingQueue(workerId)), 0, jobId);
    }

    return { jobId, dlq: this._k(QUEUE_KEYS.DLQ) };
  }

  /**
   * Mark job as finished processing and clean up in-flight queues
   */
  async ackJob(jobId, workerId = null) {
    if (!this.isInitialized) await this.init();
    await this.client.srem(this._k(QUEUE_KEYS.PROCESSING), jobId);
    if (workerId) {
      await this.client.lrem(this._k(QUEUE_KEYS.workerProcessingQueue(workerId)), 0, jobId);
    }
  }

  /**
   * Worker heartbeat & presence registration
   */
  async recordHeartbeat(workerId, ttlSec = 10) {
    if (!this.isInitialized) await this.init();
    await this.client.set(this._k(QUEUE_KEYS.workerHeartbeat(workerId)), Date.now(), 'EX', ttlSec);
    await this.client.sadd(this._k(QUEUE_KEYS.ACTIVE_WORKERS), workerId);
  }

  async unregisterWorker(workerId) {
    if (!this.isInitialized) await this.init();
    await this.client.del(this._k(QUEUE_KEYS.workerHeartbeat(workerId)));
    await this.client.srem(this._k(QUEUE_KEYS.ACTIVE_WORKERS), workerId);
  }

  /**
   * Acquire atomic distributed lock for idempotency guard (SET NX EX)
   */
  async acquireJobLock(jobId, workerId, ttlSec = 60) {
    if (!this.isInitialized) await this.init();
    const lockKey = this._k(QUEUE_KEYS.jobLock(jobId));
    const result = await this.client.set(lockKey, workerId, 'NX', 'EX', ttlSec);
    return result === 'OK';
  }

  async releaseJobLock(jobId) {
    if (!this.isInitialized) await this.init();
    await this.client.del(this._k(QUEUE_KEYS.jobLock(jobId)));
  }

  /**
   * Check or set API-level idempotency key
   */
  async checkOrSetIdempotencyKey(key, jobId, ttlSec = 86400) {
    if (!this.isInitialized) await this.init();
    const redisKey = this._k(QUEUE_KEYS.idempotency(key));
    const result = await this.client.set(redisKey, jobId, 'NX', 'EX', ttlSec);
    if (result === 'OK') {
      return { isNew: true, jobId };
    }
    const existingJobId = await this.client.get(redisKey);
    return { isNew: false, jobId: existingJobId };
  }

  /**
   * Orphan Job Reaper Daemon:
   * Inspects all active workers. If worker heartbeat expired, reclaims in-flight jobs
   * and pushes them back to HIGH priority queue to ensure at-least-once delivery.
   */
  async reapOrphanedJobs() {
    if (!this.isInitialized) await this.init();
    const activeWorkers = await this.client.smembers(this._k(QUEUE_KEYS.ACTIVE_WORKERS));
    const reapedJobs = [];

    for (const workerId of activeWorkers) {
      const hasHeartbeat = await this.client.exists(this._k(QUEUE_KEYS.workerHeartbeat(workerId)));
      if (!hasHeartbeat) {
        const workerQueue = this._k(QUEUE_KEYS.workerProcessingQueue(workerId));
        const strandedJobs = await this.client.lrange(workerQueue, 0, -1);

        if (strandedJobs && strandedJobs.length > 0) {
          console.warn(`[OrphanReaper] ⚠️ Worker ${workerId} is dead. Reclaiming ${strandedJobs.length} stranded job(s)...`);
          for (const orphanId of strandedJobs) {
            await this.client.lpush(this._k(QUEUE_KEYS.HIGH), orphanId);
            reapedJobs.push({ workerId, jobId: orphanId });
          }
        }

        await this.client.del(workerQueue);
        await this.client.srem(this._k(QUEUE_KEYS.ACTIVE_WORKERS), workerId);
      }
    }

    return reapedJobs;
  }

  startOrphanReaper(intervalMs = 5000) {
    if (this.reaperTimer) return;

    this.reaperTimer = setInterval(async () => {
      try {
        await this.reapOrphanedJobs();
      } catch (err) {
        // Silent catch for reaper loop
      }
    }, intervalMs);

    if (this.reaperTimer.unref) {
      this.reaperTimer.unref();
    }
  }

  /**
   * Background promoter: checks for retry jobs whose runAt has passed,
   * moves them back into active priority queues
   */
  startDelayedJobPromoter(intervalMs = 1000) {
    if (this.promoterTimer) return;

    this.promoterTimer = setInterval(async () => {
      try {
        const now = Date.now();
        const dueMembers = await this.client.zrangebyscore(this._k(QUEUE_KEYS.DELAYED), 0, now);

        for (const member of dueMembers) {
          const removed = await this.client.zrem(this._k(QUEUE_KEYS.DELAYED), member);
          if (removed) {
            const { jobId, priority } = JSON.parse(member);
            const queueKey = this.getQueueKeyByPriority(priority);
            await this.client.lpush(queueKey, jobId);
          }
        }
      } catch (err) {
        // Silent catch for promoter loop
      }
    }, intervalMs);

    if (this.promoterTimer.unref) {
      this.promoterTimer.unref();
    }
  }

  /**
   * Replay DLQ jobs back into HIGH queue
   */
  async replayDLQ(limit = 100) {
    if (!this.isInitialized) await this.init();
    const replayed = [];
    for (let i = 0; i < limit; i++) {
      const jobId = await this.client.rpop(this._k(QUEUE_KEYS.DLQ));
      if (!jobId) break;
      await this.client.lpush(this._k(QUEUE_KEYS.HIGH), jobId);
      replayed.push(jobId);
    }
    return replayed;
  }

  /**
   * Fetch current lengths and metrics for all queues
   */
  async getMetrics() {
    if (!this.isInitialized) await this.init();
    const [high, medium, low, delayed, processing, dlq, activeWorkers] = await Promise.all([
      this.client.llen(this._k(QUEUE_KEYS.HIGH)),
      this.client.llen(this._k(QUEUE_KEYS.MEDIUM)),
      this.client.llen(this._k(QUEUE_KEYS.LOW)),
      this.client.zcard(this._k(QUEUE_KEYS.DELAYED)),
      this.client.scard(this._k(QUEUE_KEYS.PROCESSING)),
      this.client.llen(this._k(QUEUE_KEYS.DLQ)),
      this.client.scard(this._k(QUEUE_KEYS.ACTIVE_WORKERS)),
    ]);

    return {
      high,
      medium,
      low,
      totalPending: high + medium + low,
      delayed,
      processing,
      dlq,
      activeWorkers,
    };
  }

  async clearAll() {
    if (!this.isInitialized) await this.init();
    if (!this.keyPrefix) {
      await this.client.flushall();
    } else {
      // Clear only keys matching prefix
      const keysToDelete = [
        this._k(QUEUE_KEYS.HIGH),
        this._k(QUEUE_KEYS.MEDIUM),
        this._k(QUEUE_KEYS.LOW),
        this._k(QUEUE_KEYS.DELAYED),
        this._k(QUEUE_KEYS.PROCESSING),
        this._k(QUEUE_KEYS.DLQ),
        this._k(QUEUE_KEYS.ACTIVE_WORKERS),
      ];
      await this.client.del(...keysToDelete);
    }
  }

  async close() {
    if (this.promoterTimer) clearInterval(this.promoterTimer);
    if (this.reaperTimer) clearInterval(this.reaperTimer);
    if (this.client && typeof this.client.quit === 'function') await this.client.quit();
    if (this.blockingClient && typeof this.blockingClient.quit === 'function') await this.blockingClient.quit();
    this.isInitialized = false;
  }
}

const defaultQueue = new JobQueue();

module.exports = {
  JobQueue,
  defaultQueue,
};
