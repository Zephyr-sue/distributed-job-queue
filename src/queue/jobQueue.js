const { QUEUE_KEYS, JOB_PRIORITY } = require('./constants');
const { createRedisClient } = require('./redis');

class JobQueue {
  constructor() {
    this.client = null;
    this.blockingClient = null;
    this.isInitialized = false;
    this.promoterTimer = null;
  }

  async init(label = 'main') {
    if (this.isInitialized) return;
    this.client = await createRedisClient(`${label}-client`);
    this.blockingClient = await createRedisClient(`${label}-blocking`);
    this.isInitialized = true;
    this.startDelayedJobPromoter();
  }

  getQueueKeyByPriority(priority) {
    switch (priority) {
      case JOB_PRIORITY.HIGH:
        return QUEUE_KEYS.HIGH;
      case JOB_PRIORITY.LOW:
        return QUEUE_KEYS.LOW;
      case JOB_PRIORITY.MEDIUM:
      default:
        return QUEUE_KEYS.MEDIUM;
    }
  }

  /**
   * Enqueue a job ID into the appropriate priority queue (LPUSH)
   */
  async enqueue(jobId, priority = JOB_PRIORITY.MEDIUM) {
    if (!this.isInitialized) await this.init();
    const queueKey = this.getQueueKeyByPriority(priority);
    // LPUSH pushes to head; workers BRPOP from tail (FIFO within priority tier)
    await this.client.lpush(queueKey, jobId);
    return { queueKey, jobId, priority };
  }

  /**
   * Atomically fetch the highest priority available job
   * Uses Redis BRPOP queue:HIGH queue:MEDIUM queue:LOW
   * Redis checks keys strictly in the provided order!
   */
  async fetchNext(timeoutSec = 2) {
    if (!this.isInitialized) await this.init();
    
    // Priority order: HIGH -> MEDIUM -> LOW
    const result = await this.blockingClient.brpop(
      QUEUE_KEYS.HIGH,
      QUEUE_KEYS.MEDIUM,
      QUEUE_KEYS.LOW,
      timeoutSec
    );

    if (!result) return null;

    const [queueKey, jobId] = result;
    let priority = JOB_PRIORITY.MEDIUM;
    if (queueKey === QUEUE_KEYS.HIGH) priority = JOB_PRIORITY.HIGH;
    if (queueKey === QUEUE_KEYS.LOW) priority = JOB_PRIORITY.LOW;

    // Track as actively in-flight
    await this.client.sadd(QUEUE_KEYS.PROCESSING, jobId);

    return { jobId, priority, queueKey };
  }

  /**
   * Schedule a job for delayed retry using Redis Sorted Set (ZSET)
   */
  async scheduleRetry(jobId, priority, delayMs) {
    if (!this.isInitialized) await this.init();
    const runAt = Date.now() + delayMs;
    const member = JSON.stringify({ jobId, priority });
    await this.client.zadd(QUEUE_KEYS.DELAYED, runAt, member);
    await this.client.srem(QUEUE_KEYS.PROCESSING, jobId);
    return { jobId, runAt };
  }

  /**
   * Mark job as finished processing
   */
  async ackJob(jobId) {
    if (!this.isInitialized) await this.init();
    await this.client.srem(QUEUE_KEYS.PROCESSING, jobId);
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
        // Get due jobs
        const dueMembers = await this.client.zrangebyscore(QUEUE_KEYS.DELAYED, 0, now);

        for (const member of dueMembers) {
          // Remove from ZSET first to prevent double-processing
          const removed = await this.client.zrem(QUEUE_KEYS.DELAYED, member);
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
   * Fetch current lengths and metrics for all queues
   */
  async getMetrics() {
    if (!this.isInitialized) await this.init();
    const [high, medium, low, delayed, processing] = await Promise.all([
      this.client.llen(QUEUE_KEYS.HIGH),
      this.client.llen(QUEUE_KEYS.MEDIUM),
      this.client.llen(QUEUE_KEYS.LOW),
      this.client.zcard(QUEUE_KEYS.DELAYED),
      this.client.scard(QUEUE_KEYS.PROCESSING),
    ]);

    return {
      high,
      medium,
      low,
      totalPending: high + medium + low,
      delayed,
      processing,
    };
  }

  async clearAll() {
    if (!this.isInitialized) await this.init();
    await this.client.flushall();
  }

  async close() {
    if (this.promoterTimer) clearInterval(this.promoterTimer);
    if (this.client) await this.client.quit();
    if (this.blockingClient) await this.blockingClient.quit();
    this.isInitialized = false;
  }
}

const defaultQueue = new JobQueue();

module.exports = {
  JobQueue,
  defaultQueue,
};
