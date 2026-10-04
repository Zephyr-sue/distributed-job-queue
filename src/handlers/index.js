const { JOB_TYPES } = require('../queue/constants');
const { handleEmail } = require('./emailHandler');
const { handleReport } = require('./reportHandler');
const { handleImageResize } = require('./imageResizeHandler');
const { sleep } = require('../utils/helpers');

const handlers = {
  [JOB_TYPES.EMAIL]: handleEmail,
  [JOB_TYPES.REPORT_GENERATION]: handleReport,
  [JOB_TYPES.IMAGE_RESIZE]: handleImageResize,
  [JOB_TYPES.WEBHOOK_DISPATCH]: async (payload) => {
    await sleep(100);
    return { status: 200, acknowledged: true };
  },
  [JOB_TYPES.FAILING_SIMULATION]: async (payload, job) => {
    await sleep(100);
    // If job payload has succeedOnAttempt, we can test retry recovery!
    if (payload.succeedOnAttempt && job.attempts >= payload.succeedOnAttempt) {
      return { recovered: true, successfulAttempt: job.attempts };
    }
    throw new Error(payload.errorMessage || 'Intentional simulated worker task failure');
  },
};

function getHandler(jobType) {
  const handler = handlers[jobType];
  if (!handler) {
    throw new Error(`No registered handler found for job type: "${jobType}"`);
  }
  return handler;
}

module.exports = {
  getHandler,
  handlers,
};
