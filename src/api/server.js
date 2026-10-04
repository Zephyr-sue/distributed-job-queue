const express = require('express');
const cors = require('cors');
const path = require('path');
const config = require('../config');
const { connectDB } = require('../database/mongo');
const { defaultQueue } = require('../queue/jobQueue');
const jobRoutes = require('./routes/jobRoutes');
const metricRoutes = require('./routes/metricRoutes');

const app = express();

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// API Routes
app.use('/api/jobs', jobRoutes);
app.use('/api/metrics', metricRoutes);

// Healthcheck
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

async function startServer() {
  try {
    await connectDB();
    await defaultQueue.init('api-server');

    // Automatically launch embedded worker pool in local mode
    const { WorkerProcess } = require('../worker/worker');
    const workerCount = config.worker.concurrency || 3;
    console.log(`👷 [Embedded Workers] Spawning ${workerCount} background workers...`);
    const activeWorkers = [];
    for (let i = 1; i <= workerCount; i++) {
      const worker = new WorkerProcess(`worker-#${i}`);
      activeWorkers.push(worker);
      worker.start().catch((err) => console.error(`Worker #${i} error:`, err));
    }

    const server = app.listen(config.port, () => {
      console.log(`====================================================`);
      console.log(`🌐 Distributed Job Queue Server & Workers RUNNING!`);
      console.log(`📍 Dashboard URL: http://localhost:${config.port}`);
      console.log(`⚡ Concurrency: ${workerCount} Active Concurrent Workers`);
      console.log(`📚 Healthcheck: http://localhost:${config.port}/health`);
      console.log(`====================================================`);
    });

    const shutdown = async () => {
      console.log('\n🛑 Shutting down API server and workers...');
      activeWorkers.forEach((w) => (w.isRunning = false));
      server.close();
      await defaultQueue.close();
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  } catch (err) {
    console.error('Failed to start API server:', err);
    process.exit(1);
  }
}

if (require.main === module) {
  startServer();
}

module.exports = { app, startServer };
