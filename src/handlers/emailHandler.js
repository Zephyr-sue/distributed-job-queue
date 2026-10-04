const { sleep } = require('../utils/helpers');

async function handleEmail(payload, job) {
  const { to, subject = 'Notification', template = 'welcome' } = payload;
  if (!to) throw new Error('Email recipient "to" address is required');

  // Simulate network I/O & email template compilation
  await sleep(150 + Math.random() * 200);

  return {
    messageId: `msg_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
    recipient: to,
    status: 'DELIVERED',
    provider: 'SMTP-Virtual-Relay',
    deliveredAt: new Date().toISOString(),
  };
}

module.exports = { handleEmail };
