const { connectDB } = require('../src/database/mongo');
const jobService = require('../src/services/jobService');
const { JOB_PRIORITY, JOB_TYPES } = require('../src/queue/constants');
const { defaultQueue } = require('../src/queue/jobQueue');

async function seed() {
  console.log('🌱 Seeding initial demo jobs into Distributed Job Queue...');
  await connectDB();
  await defaultQueue.init('seed-script');

  const demoJobs = [
    {
      type: JOB_TYPES.EMAIL,
      priority: JOB_PRIORITY.HIGH,
      payload: { to: 'founder@startup.io', subject: 'Urgent: Server Health Alert', template: 'alert_email' },
    },
    {
      type: JOB_TYPES.IMAGE_RESIZE,
      priority: JOB_PRIORITY.HIGH,
      payload: { imageUrl: 'https://images.unsplash.com/photo-1579546929518-9e396f3cc809', dimensions: { width: 1920, height: 1080 } },
    },
    {
      type: JOB_TYPES.REPORT_GENERATION,
      priority: JOB_PRIORITY.MEDIUM,
      payload: { reportType: 'QUARTERLY_TAX_AUDIT', userId: 'org_4492' },
    },
    {
      type: JOB_TYPES.EMAIL,
      priority: JOB_PRIORITY.MEDIUM,
      payload: { to: 'customer@gmail.com', subject: 'Your Monthly Statement', template: 'statement' },
    },
    {
      type: JOB_TYPES.FAILING_SIMULATION,
      priority: JOB_PRIORITY.LOW,
      payload: { errorMessage: 'Simulated 3rd party webhook timeout', succeedOnAttempt: 3 },
      maxRetries: 3,
    },
    {
      type: JOB_TYPES.REPORT_GENERATION,
      priority: JOB_PRIORITY.LOW,
      payload: { reportType: 'ARCHIVED_LOG_EXPORT', userId: 'admin_1' },
    },
  ];

  for (const job of demoJobs) {
    const created = await jobService.submitJob(job);
    console.log(`✅ Seeded: ${created.type} [${created.priority}] -> ID: ${created.jobId}`);
  }

  console.log(`\n🎉 Seeded ${demoJobs.length} demo jobs. Run "npm run start:worker" or open Dashboard to view execution!`);
  await defaultQueue.close();
  process.exit(0);
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
