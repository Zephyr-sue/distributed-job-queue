const API_BASE = '/api';
let pollInterval = null;

async function fetchMetrics() {
  try {
    const res = await fetch(`${API_BASE}/metrics`);
    if (!res.ok) return;
    const data = await res.json();

    document.getElementById('valTotal').innerText = data.database.total || 0;
    document.getElementById('valPending').innerText = data.queue.totalPending || 0;
    document.getElementById('badgeHigh').innerText = `HIGH: ${data.queue.high || 0}`;
    document.getElementById('badgeMed').innerText = `MED: ${data.queue.medium || 0}`;
    document.getElementById('badgeLow').innerText = `LOW: ${data.queue.low || 0}`;
    document.getElementById('valProcessing').innerText = data.database.processing || 0;
    document.getElementById('valDelayed').innerText = `Retrying: ${data.database.retrying || 0}`;
    document.getElementById('valCompleted').innerText = data.database.completed || 0;
    document.getElementById('valFailed').innerText = data.database.failed || 0;
    document.getElementById('valSuccessRate').innerText = `Success Rate: ${data.database.successRate || '100%'}`;
    document.getElementById('valAvgTime').innerText = `Avg Time: ${data.performance.avgExecutionTimeMs || 0}ms`;
  } catch (err) {
    console.error('Error fetching metrics:', err);
  }
}

async function fetchJobs() {
  try {
    const filter = document.getElementById('filterStatus').value;
    const url = filter ? `${API_BASE}/jobs?status=${filter}&limit=40` : `${API_BASE}/jobs?limit=40`;
    const res = await fetch(url);
    if (!res.ok) return;
    const { jobs } = await res.json();

    const tbody = document.getElementById('jobTableBody');
    if (!jobs || jobs.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No jobs in system</td></tr>`;
      return;
    }

    tbody.innerHTML = jobs.map((job) => {
      const shortId = job.jobId.replace('job_', '').substring(0, 8);
      const duration = job.executionTimeMs ? `${job.executionTimeMs}ms` : '-';
      const retryBtn = job.status === 'FAILED'
        ? `<button class="btn btn-sm btn-warn" onclick="retryJob('${job.jobId}')">🔄 Retry</button>`
        : '';

      return `
        <tr>
          <td title="${job.jobId}">${shortId}...</td>
          <td><b>${job.type}</b></td>
          <td><span class="pill pill-${job.priority.toLowerCase()}">${job.priority}</span></td>
          <td><span class="status-badge status-${job.status}">${job.status}</span></td>
          <td>${job.attempts || 0}/${job.maxRetries || 3}</td>
          <td>${duration}</td>
          <td>${retryBtn}</td>
        </tr>
      `;
    }).join('');
  } catch (err) {
    console.error('Error fetching jobs:', err);
  }
}

async function submitJob(data) {
  try {
    const res = await fetch(`${API_BASE}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    const result = await res.json();
    fetchMetrics();
    fetchJobs();
    return result;
  } catch (err) {
    alert('Failed to submit job: ' + err.message);
  }
}

async function submitQuickJob(type, priority) {
  let payload = {};
  if (type === 'EMAIL') {
    payload = { to: `user_${Math.floor(Math.random()*1000)}@company.com`, subject: 'Invoice Receipt' };
  } else if (type === 'REPORT_GENERATION') {
    payload = { reportType: 'ANNUAL_AUDIT', userId: 'usr_892' };
  } else if (type === 'IMAGE_RESIZE') {
    payload = { imageUrl: 'https://cdn.app/assets/banner.png', dimensions: { width: 1200, height: 800 } };
  } else if (type === 'FAILING_SIMULATION') {
    payload = { errorMessage: 'Simulated downstream API timeout', succeedOnAttempt: 3 };
  }

  await submitJob({ type, payload, priority, maxRetries: 3 });
}

async function simulateBurst(count = 30) {
  const jobs = [];
  const types = ['EMAIL', 'REPORT_GENERATION', 'IMAGE_RESIZE', 'FAILING_SIMULATION'];
  const priorities = ['HIGH', 'MEDIUM', 'LOW'];

  for (let i = 0; i < count; i++) {
    const priority = priorities[Math.floor(Math.random() * priorities.length)];
    let type = types[Math.floor(Math.random() * (types.length - 1))]; // mostly regular jobs
    let payload = { index: i, timestamp: Date.now() };

    // 15% chance of transient retry job, 5% chance of permanent failure to test manual retries
    const rand = Math.random();
    if (rand < 0.05) {
      type = 'FAILING_SIMULATION';
      payload.errorMessage = 'External payment gateway 503 Service Unavailable';
      payload.succeedOnAttempt = null; // Permanently fails after 3 retries
    } else if (rand < 0.20) {
      type = 'FAILING_SIMULATION';
      payload.errorMessage = 'Transient 3rd party SMTP socket timeout';
      payload.succeedOnAttempt = 2; // Auto-recovers on 2nd attempt via backoff
    }

    if (type === 'EMAIL') {
      payload.to = `burst_user_${i}@example.com`;
      payload.subject = `Burst Email Notification #${i}`;
    } else if (type === 'IMAGE_RESIZE') {
      payload.imageUrl = `https://cdn.example.com/images/asset_${i}.jpg`;
      payload.dimensions = { width: 800, height: 600 };
    } else if (type === 'REPORT_GENERATION') {
      payload.reportType = 'PERFORMANCE_SUMMARY';
      payload.userId = `user_${i}`;
    }

    jobs.push({
      type,
      priority,
      payload,
      maxRetries: 3,
    });
  }

  await fetch(`${API_BASE}/jobs/bulk`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jobs }),
  });

  fetchMetrics();
  fetchJobs();
}

async function retryJob(jobId) {
  await fetch(`${API_BASE}/jobs/${jobId}/retry`, { method: 'POST' });
  fetchMetrics();
  fetchJobs();
}

async function clearSystem() {
  if (!confirm('Are you sure you want to clear all jobs and queues?')) return;
  await fetch(`${API_BASE}/jobs/clear`, { method: 'DELETE' });
  fetchMetrics();
  fetchJobs();
}

document.getElementById('jobForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const type = document.getElementById('jobType').value;
  const priority = document.getElementById('jobPriority').value;
  const maxRetries = parseInt(document.getElementById('maxRetries').value, 10);
  let payload = {};
  try {
    payload = JSON.parse(document.getElementById('jobPayload').value);
  } catch (err) {
    alert('Invalid JSON in payload field');
    return;
  }

  await submitJob({ type, priority, maxRetries, payload });
});

document.getElementById('btnRefresh').addEventListener('click', () => {
  fetchMetrics();
  fetchJobs();
});

document.getElementById('btnClear').addEventListener('click', clearSystem);
document.getElementById('filterStatus').addEventListener('change', fetchJobs);

// Auto Polling every 1.5 seconds
function startPolling() {
  fetchMetrics();
  fetchJobs();
  pollInterval = setInterval(() => {
    fetchMetrics();
    fetchJobs();
  }, 1500);
}

startPolling();
