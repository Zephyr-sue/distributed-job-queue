const { getJobRepository } = require('../models/Job');
const { defaultQueue } = require('../queue/jobQueue');
const { JOB_STATUS } = require('../queue/constants');

class MetricService {
  async getSystemMetrics() {
    const JobRepo = getJobRepository();
    const queueMetrics = await defaultQueue.getMetrics();

    // Query DB status counts
    const [pending, processing, completed, failed, retrying] = await Promise.all([
      JobRepo.countDocuments({ status: JOB_STATUS.PENDING }),
      JobRepo.countDocuments({ status: JOB_STATUS.PROCESSING }),
      JobRepo.countDocuments({ status: JOB_STATUS.COMPLETED }),
      JobRepo.countDocuments({ status: JOB_STATUS.FAILED }),
      JobRepo.countDocuments({ status: JOB_STATUS.RETRYING }),
    ]);

    const totalJobs = pending + processing + completed + failed + retrying;

    // Calculate recent execution latency from completed jobs
    const recentCompleted = await JobRepo.find(
      { status: JOB_STATUS.COMPLETED },
      { limit: 20 }
    );

    let avgExecutionTimeMs = 0;
    if (recentCompleted.length > 0) {
      const sum = recentCompleted.reduce((acc, j) => acc + (j.executionTimeMs || 0), 0);
      avgExecutionTimeMs = Math.round(sum / recentCompleted.length);
    }

    return {
      timestamp: new Date().toISOString(),
      queue: queueMetrics,
      database: {
        total: totalJobs,
        pending,
        processing,
        completed,
        failed,
        retrying,
        successRate: totalJobs > 0 ? ((completed / (completed + failed || 1)) * 100).toFixed(1) + '%' : '100%',
      },
      performance: {
        avgExecutionTimeMs,
        sampleSize: recentCompleted.length,
      },
    };
  }
}

module.exports = new MetricService();
