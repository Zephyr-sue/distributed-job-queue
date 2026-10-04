const crypto = require('crypto');

const CATEGORIES = ['SUBSCRIPTION', 'CLOUD_HOSTING', 'API_CREDITS', 'ENTERPRISE_LICENSE', 'REFUND'];

function generateUserTransactions(userId, count = 1500) {
  const transactions = [];
  for (let i = 0; i < count; i++) {
    const category = CATEGORIES[i % CATEGORIES.length];
    let amount = 0;
    if (category === 'SUBSCRIPTION') amount = 29.99 + (i % 5) * 10;
    else if (category === 'CLOUD_HOSTING') amount = 120.50 + (i % 50) * 15.2;
    else if (category === 'ENTERPRISE_LICENSE') amount = 1500.00 + (i % 10) * 100;
    else if (category === 'REFUND') amount = -49.99;
    else amount = 9.99 + (i % 20);

    transactions.push({
      transactionId: `tx_${userId}_${i}`,
      userId,
      category,
      amount: parseFloat(amount.toFixed(2)),
      timestamp: new Date(Date.now() - (count - i) * 3600000).toISOString(),
    });
  }
  return transactions;
}

async function handleReport(payload, job) {
  const { reportType = 'MONTHLY_SUMMARY', userId = 'usr_1', dataRows = [] } = payload;

  const transactions = dataRows && dataRows.length > 0 ? dataRows : generateUserTransactions(userId, 1500);
  const rowCount = transactions.length;

  let totalRevenue = 0;
  const values = [];
  const categoryTotals = {};

  // 1. Process and categorize every single transaction
  for (let i = 0; i < rowCount; i++) {
    const tx = transactions[i];
    const amount = typeof tx === 'number' ? tx : (tx.amount || 0);
    const cat = tx.category || 'GENERAL';

    totalRevenue += amount;
    values.push(amount);
    categoryTotals[cat] = (categoryTotals[cat] || 0) + amount;
  }

  // 2. Statistical calculations: Mean, Variance & Standard Deviation
  const mean = totalRevenue / rowCount;
  const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / rowCount;
  const stdDev = Math.sqrt(variance);

  // 3. Serialize report structure & calculate verification hash
  const summary = {
    reportType,
    userId,
    totalTransactions: rowCount,
    grossRevenue: parseFloat(totalRevenue.toFixed(2)),
    meanTransaction: parseFloat(mean.toFixed(2)),
    standardDeviation: parseFloat(stdDev.toFixed(2)),
    categoryBreakdown: Object.fromEntries(
      Object.entries(categoryTotals).map(([k, v]) => [k, parseFloat(v.toFixed(2))])
    ),
    generatedAt: new Date().toISOString(),
  };

  const reportHash = crypto.createHash('sha256').update(JSON.stringify(summary)).digest('hex');

  return {
    reportId: `rep_${reportHash.substring(0, 12)}`,
    type: reportType,
    pages: Math.ceil(rowCount / 200),
    metrics: summary,
    verificationHash: reportHash,
  };
}

module.exports = { handleReport, generateUserTransactions };
