const JOB_STATUS = Object.freeze({
  PENDING: 'PENDING',
  PROCESSING: 'PROCESSING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
  RETRYING: 'RETRYING',
});

const JOB_PRIORITY = Object.freeze({
  HIGH: 'HIGH',
  MEDIUM: 'MEDIUM',
  LOW: 'LOW',
});

const JOB_TYPES = Object.freeze({
  EMAIL: 'EMAIL',
  REPORT_GENERATION: 'REPORT_GENERATION',
  IMAGE_RESIZE: 'IMAGE_RESIZE',
  WEBHOOK_DISPATCH: 'WEBHOOK_DISPATCH',
  FAILING_SIMULATION: 'FAILING_SIMULATION',
});

const QUEUE_KEYS = Object.freeze({
  HIGH: 'queue:jobs:HIGH',
  MEDIUM: 'queue:jobs:MEDIUM',
  LOW: 'queue:jobs:LOW',
  DELAYED: 'zset:jobs:delayed',
  PROCESSING: 'set:jobs:processing',
});

module.exports = {
  JOB_STATUS,
  JOB_PRIORITY,
  JOB_TYPES,
  QUEUE_KEYS,
};
