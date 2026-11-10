require('dotenv').config();

module.exports = {
  port: parseInt(process.env.PORT, 10) || 3000,
  nodeEnv: process.env.NODE_ENV || 'development',
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
  },
  mongo: {
    uri: process.env.MONGO_URI || 'mongodb://localhost:27017/job_queue_db',
  },
  worker: {
    concurrency: parseInt(process.env.WORKER_CONCURRENCY, 10) || 5,
    defaultMaxRetries: parseInt(process.env.DEFAULT_MAX_RETRIES, 10) || 3,
    defaultRetryBackoffMs: parseInt(process.env.DEFAULT_RETRY_BACKOFF_MS, 10) || 2000,
    pollTimeoutSec: parseInt(process.env.POLL_TIMEOUT_SEC, 10) || 2,
    heartbeatIntervalMs: parseInt(process.env.WORKER_HEARTBEAT_INTERVAL_MS, 10) || 3000,
    heartbeatTtlSec: parseInt(process.env.WORKER_HEARTBEAT_TTL_SEC, 10) || 10,
    orphanReaperIntervalMs: parseInt(process.env.ORPHAN_REAPER_INTERVAL_MS, 10) || 5000,
    jobLockTtlSec: parseInt(process.env.JOB_LOCK_TTL_SEC, 10) || 60,
  },
  queue: {
    maxQueueDepth: parseInt(process.env.MAX_QUEUE_DEPTH, 10) || 10000,
  },
  enableInMemoryFallback: process.env.ENABLE_IN_MEMORY_FALLBACK !== 'false',
};
