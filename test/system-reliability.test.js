const assert = require('assert');
const { JobQueue } = require('../src/queue/jobQueue');
const { JOB_PRIORITY, JOB_STATUS, JOB_TYPES, QUEUE_KEYS } = require('../src/queue/constants');
const jobService = require('../src/services/jobService');
const { WorkerProcess } = require('../src/worker/worker');
const config = require('../src/config');
const { connectDB } = require('../src/database/mongo');
const { getJobRepository } = require('../src/models/Job');

async function runTests() {
  console.log('🧪 Starting System Reliability & Verification Test Suite...\n');
  await connectDB();

  // Use isolated test queue prefix so background Docker workers don't steal test jobs
  const testQueue = new JobQueue({ keyPrefix: 'test' });
  await testQueue.init('test-suite');
  await testQueue.clearAll();
  jobService.setQueue(testQueue);

  const JobRepo = getJobRepository();
  await JobRepo.deleteMany({});

  // Test 1: Atomic Worker Handoff (BLMOVE / LMOVE)
  console.log('▶ Test 1: Atomic worker handoff using LMOVE / BLMOVE');
  const testJobId1 = `job_test_${Date.now()}`;
  await JobRepo.create({
    jobId: testJobId1,
    type: JOB_TYPES.EMAIL,
    payload: { to: 'worker_test@example.com' },
    priority: JOB_PRIORITY.HIGH,
    status: JOB_STATUS.PENDING,
  });

  await testQueue.enqueue(testJobId1, JOB_PRIORITY.HIGH);
  const worker1Id = 'worker-test-alpha';
  const fetched = await testQueue.fetchNext(worker1Id, 2);
  assert.ok(fetched, 'fetchNext should return a fetched job item');
  assert.strictEqual(fetched.jobId, testJobId1, 'Fetched job must match enqueued high priority job');
  
  // Verify job is now atomically in the worker's processing queue and not in HIGH queue
  const highLen = await testQueue.client.llen(testQueue.getQueueKeyByPriority(JOB_PRIORITY.HIGH));
  const inFlightJobs = await testQueue.client.lrange(testQueue._k(QUEUE_KEYS.workerProcessingQueue(worker1Id)), 0, -1);
  assert.strictEqual(highLen, 0, 'HIGH queue should be empty after atomic move');
  assert.deepStrictEqual(inFlightJobs, [testJobId1], 'Job must be in worker-specific in-flight queue');

  // Acknowledge job
  await testQueue.ackJob(testJobId1, worker1Id);
  const postAckInFlight = await testQueue.client.lrange(testQueue._k(QUEUE_KEYS.workerProcessingQueue(worker1Id)), 0, -1);
  assert.strictEqual(postAckInFlight.length, 0, 'In-flight queue must be cleared on ack');
  console.log('  ✔ Atomic handoff and ack verified.\n');

  // Test 2: Worker Heartbeat & Orphan Job Reaper
  console.log('▶ Test 2: Worker Heartbeat and Orphan Job Reclamation Daemon');
  const crashedWorkerId = 'worker-crashed-xyz';
  // Simulate worker picking up a job into its in-flight queue
  await testQueue.client.lpush(testQueue._k(QUEUE_KEYS.workerProcessingQueue(crashedWorkerId)), 'job_stranded_101');
  await testQueue.client.sadd(testQueue._k(QUEUE_KEYS.ACTIVE_WORKERS), crashedWorkerId);
  // Do NOT write heartbeat (simulates dead / crashed worker)

  const reaped = await testQueue.reapOrphanedJobs();
  assert.strictEqual(reaped.length, 1, 'Reaper should reclaim exactly 1 orphaned job');
  assert.strictEqual(reaped[0].jobId, 'job_stranded_101');
  assert.strictEqual(reaped[0].workerId, crashedWorkerId);

  // Check that orphaned job is now back in HIGH queue for immediate reprocessing
  const reclaimedHigh = await testQueue.client.lrange(testQueue.getQueueKeyByPriority(JOB_PRIORITY.HIGH), 0, -1);
  assert.strictEqual(reclaimedHigh.includes('job_stranded_101'), true, 'Stranded job must be repushed to HIGH queue');
  console.log('  ✔ Orphan job reaper successfully reclaimed stranded job from dead worker.\n');

  // Test 3: Idempotency Lock Guard (SET NX EX) & API Deduplication
  console.log('▶ Test 3: Distributed Idempotency Lock Guard (SET NX EX) & Key Deduplication');
  const lockAcquiredFirst = await testQueue.acquireJobLock('job_concurrent_1', 'worker-A', 5);
  const lockAcquiredSecond = await testQueue.acquireJobLock('job_concurrent_1', 'worker-B', 5);
  assert.strictEqual(lockAcquiredFirst, true, 'First worker must acquire lock');
  assert.strictEqual(lockAcquiredSecond, false, 'Second worker must be rejected by idempotency lock');
  await testQueue.releaseJobLock('job_concurrent_1');
  const lockAcquiredAfterRelease = await testQueue.acquireJobLock('job_concurrent_1', 'worker-B', 5);
  assert.strictEqual(lockAcquiredAfterRelease, true, 'Worker B must acquire lock after release');
  await testQueue.releaseJobLock('job_concurrent_1');

  // API Idempotency Key deduplication
  const idempKey = `pay_tx_${Date.now()}`;
  const job1 = await jobService.submitJob({
    type: JOB_TYPES.EMAIL,
    payload: { to: 'test@example.com' },
    idempotencyKey: idempKey,
  });
  const job2 = await jobService.submitJob({
    type: JOB_TYPES.EMAIL,
    payload: { to: 'test@example.com' },
    idempotencyKey: idempKey,
  });
  assert.strictEqual(job1.jobId, job2.jobId, 'Submitting identical idempotencyKey must return existing job');
  assert.strictEqual(job2.isIdempotentDuplicate, true, 'Second submission must be flagged as idempotent duplicate');
  console.log('  ✔ Idempotency locks and API deduplication verified.\n');

  // Test 4: Dead Letter Queue (DLQ) & Exponential Backoff with Jitter
  console.log('▶ Test 4: Exponential Backoff & Dead Letter Queue (DLQ) routing');
  const failingJob = await jobService.submitJob({
    type: JOB_TYPES.FAILING_SIMULATION,
    payload: { errorMessage: 'Simulated hard crash', succeedOnAttempt: 99 },
    maxRetries: 2,
    backoffMs: 50,
  });

  const testWorker = new WorkerProcess('worker-dlq-tester', { queue: testQueue });
  await testWorker.queue.init('worker-dlq-tester');

  // Attempt 1 -> should fail and schedule retry
  await testWorker.processJob(failingJob.jobId, JOB_PRIORITY.MEDIUM);
  let updatedJob = await JobRepo.findOne({ jobId: failingJob.jobId });
  assert.strictEqual(updatedJob.status, JOB_STATUS.RETRYING, 'Job should be RETRYING after attempt 1');

  // Force promote from ZSET to active queue
  await testQueue.client.zrem(testQueue._k(QUEUE_KEYS.DELAYED), JSON.stringify({ jobId: failingJob.jobId, priority: JOB_PRIORITY.MEDIUM }));
  await testQueue.enqueue(failingJob.jobId, JOB_PRIORITY.HIGH);

  // Attempt 2 -> retries exhausted, should route to DLQ
  await testWorker.processJob(failingJob.jobId, JOB_PRIORITY.HIGH);
  updatedJob = await JobRepo.findOne({ jobId: failingJob.jobId });
  assert.strictEqual(updatedJob.status, JOB_STATUS.DLQ, 'Job should be DLQ after exhausting maxRetries');

  const dlqItems = await testQueue.client.lrange(testQueue._k(QUEUE_KEYS.DLQ), 0, -1);
  assert.strictEqual(dlqItems.includes(failingJob.jobId), true, 'Job must exist in Redis DLQ list');

  // Replay DLQ
  const replayed = await testQueue.replayDLQ(10);
  assert.strictEqual(replayed.includes(failingJob.jobId), true, 'DLQ job should be replayed back to active queue');
  console.log('  ✔ DLQ routing and DLQ replay verified.\n');

  // Test 5: Producer Backpressure & Capacity Throttling
  console.log('▶ Test 5: Producer Backpressure & Queue-Depth Throttling');
  const originalMax = config.queue.maxQueueDepth;
  config.queue.maxQueueDepth = 2; // set temporary low threshold for testing

  await testQueue.enqueue('fill_1', JOB_PRIORITY.MEDIUM);
  await testQueue.enqueue('fill_2', JOB_PRIORITY.MEDIUM);

  let backpressureTriggered = false;
  try {
    await jobService.submitJob({
      type: JOB_TYPES.EMAIL,
      payload: { to: 'overflow@example.com' },
    });
  } catch (err) {
    if (err.isBackpressure) {
      backpressureTriggered = true;
    }
  }

  assert.strictEqual(backpressureTriggered, true, 'Producer must throw BackpressureError when depth exceeds limit');
  config.queue.maxQueueDepth = originalMax; // restore
  console.log('  ✔ Producer backpressure throttling verified.\n');

  console.log('🎉 ALL 5 SYSTEM RELIABILITY TESTS PASSED WITH 100% SUCCESS!\n');
  await testQueue.clearAll();
  await testQueue.close();
  process.exit(0);
}

runTests().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
