const express = require('express');
const router = express.Router();
const metricService = require('../../services/metricService');

/**
 * GET /api/metrics - Real-time queue and cluster stats
 */
router.get('/', async (req, res) => {
  try {
    const metrics = await metricService.getSystemMetrics();
    return res.json(metrics);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;
