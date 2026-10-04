const { WorkerProcess } = require('./worker');
const config = require('../config');

const poolSize = config.worker.concurrency || 3;
console.log(`🏭 Initializing Worker Pool with ${poolSize} concurrent workers...`);

const workers = [];
for (let i = 1; i <= poolSize; i++) {
  const worker = new WorkerProcess(`pool-worker-#${i}`);
  workers.push(worker);
  worker.start().catch((err) => {
    console.error(`Failed to start worker #${i}:`, err);
  });
}

console.log(`🎉 Worker Pool running with ${workers.length} active workers.`);
