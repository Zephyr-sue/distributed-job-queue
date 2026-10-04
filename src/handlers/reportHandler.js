const { sleep } = require('../utils/helpers');

async function handleReport(payload, job) {
  const { reportType = 'MONTHLY_SUMMARY', userId, dateRange } = payload;

  // Simulate heavy compute / DB query aggregation
  await sleep(350 + Math.random() * 300);

  return {
    reportId: `rep_${Date.now()}`,
    type: reportType,
    pages: Math.floor(Math.random() * 10) + 1,
    downloadUrl: `https://storage.internal.app/reports/${reportType.toLowerCase()}_${Date.now()}.pdf`,
    generatedAt: new Date().toISOString(),
  };
}

module.exports = { handleReport };
