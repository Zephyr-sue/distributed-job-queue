# ⚡ Distributed Job Queue

A production-grade, distributed background job processing engine built with **JavaScript / Node.js**, **Redis**, **MongoDB**, and **Docker**. Features atomic multi-tier priority scheduling, self-healing exponential backoff retries, persistent execution audit trails, real-time dark-mode web telemetry, and horizontal worker scaling.

---

## 🎯 Architecture Overview

```
                      POST /api/jobs (HTTP)
                             │
                             ▼
                    ┌─────────────────┐
                    │  Node.js API    │
                    │  (Express.js)   │
                    └────────┬────────┘
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
   ┌─────────────────┐               ┌─────────────────┐
   │  Redis Priority │               │    MongoDB      │
   │  Queues & ZSET  │               │  State & Logs   │
   │ HIGH | MED | LOW│               │   Persistence   │
   └────────┬────────┘               └────────┬────────┘
            │                                 │
     ┌──────┴──────────────┐                  │
     ▼                     ▼                  │
┌──────────┐          ┌──────────┐            │
│ Worker 1 │          │ Worker 2 │ ◄──────────┘ (Updates status,
└──────────┘          └──────────┘               attempts & logs)
```

---

## 🖼️ Concrete Job Pipelines

1. **`IMAGE_RESIZE` (Profile Avatar Center-Crop & Normalization)**:
   - Takes 4 diverse raw user photo formats (4K Landscape `3840x2160`, Vertical Phone Selfie `1080x1920`, Square `1080x1080`, and DSLR `1600x1200`).
   - Calculates mathematical center-crop bounding box to preserve aspect ratios without stretching faces.
   - Executes real C++/Bilinear interpolation and `zlib` DEFLATE compression to output standard **$150 \times 150$ Square Avatar Thumbnails**.
2. **`REPORT_GENERATION` (Multi-Tier Financial Analytics & Variance)**:
   - Processes 1,500 enterprise transaction rows across 5 financial categories (`SUBSCRIPTION`, `CLOUD_HOSTING`, `API_CREDITS`, `ENTERPRISE_LICENSE`, `REFUND`).
   - Calculates Gross Revenue, Mean ($\mu$), Variance ($\sigma^2$), and Standard Deviation ($\sigma$) with SHA-256 audit sealing.
3. **`EMAIL` (Cryptographic DKIM Signing & MIME Packaging)**:
   - Generates cryptographic HMAC-SHA256 DKIM authentication signatures over message headers and packages RFC-822 MIME streams.
4. **`FAILING_SIMULATION` (Fault Tolerance & Self-Healing)**:
   - Triggers transient 3rd-party downstream timeouts and automatically self-heals via Redis Sorted Sets (`ZSET`).

---

## 📊 Official 1,000-Job Multi-Size Benchmark Results

```bash
npm run benchmark
```

```
================================================================
📈 OFFICIAL 1,000-JOB BENCHMARK RESULTS
================================================================
• Total Jobs Processed:        1,000
• Concurrent Worker Threads:   8 parallel instances
• Ingestion Throughput:        31,250 ops/sec
• Execution Throughput:        260 jobs/sec (Real CPU Compute)
• DB Status Filter Latency:    32.30 ms (1,000 record retrieval & sort)
• Average Execution Time:      22 ms
• Latency p50 (Median):        7 ms
• Latency p90:                 108 ms
• Latency p95:                 109 ms
• Latency p99:                 304 ms
• Fault Recovery Rate:         100.0% (via Exponential Backoff)
================================================================
```

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Start API Server, Dashboard, and Embedded Workers (Port 3000)
npm start
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

---

## 📄 License
MIT
