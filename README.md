# Real-Time E-Commerce Intelligence Platform

> End-to-end real-time streaming pipeline using Google Cloud Pub/Sub,
> BigQuery, and Gemini AI that processes live orders, detects fraud
> patterns, and monitors anomalies — fully automated via GitHub Actions.

[![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![PubSub](https://img.shields.io/badge/Pub/Sub-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/pubsub)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Gemini](https://img.shields.io/badge/Gemini_AI-8E75B2?style=flat&logo=google&logoColor=white)](https://ai.google.dev)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat&logo=github-actions&logoColor=white)](https://github.com/features/actions)

---

## Live Results — One Automated Run

| Metric | Value |
|--------|-------|
| Orders processed | 656 |
| Total revenue tracked | $815,495.65 |
| Fraud events detected | 50 |
| Critical anomalies flagged | 6 |
| Pipeline speed | 1 order / second |
| Automation | 100% — Mac OFF |

---

## Real Anomalies Detected by Gemini AI

```
CRITICAL — FRAUD SPIKE
  23 fraud events in 5 minutes
  23% of total revenue was fraudulent
  Action: Immediate investigation required

CRITICAL — ORDER SURGE
  335 orders in 5 minutes
  1150% above historical average
  Action: Verify bot activity or viral campaign

CRITICAL — REVENUE SURGE
  $408,669 revenue in 5 minutes
  1100% above historical average
  Action: Validate pricing and flash sale impact

CRITICAL — HIGH FRAUD ACTIVITY
  33 fraud events in 5 minutes
  25% of revenue flagged as fraudulent
  Action: Block suspicious customer IDs

WARNING — DECREASED AVG ORDER VALUE
  $1,158 vs normal $1,453
  20% drop detected during surge
  Action: Monitor product mix changes
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              GITHUB ACTIONS (6 AM Daily)                │
│                                                         │
│   Simulator → Pub/Sub → Processor → BigQuery           │
│                                   → Anomaly Detector   │
│                                   → Daily Report       │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              ORDER SIMULATOR                            │
│   → Generates 1 order per second                       │
│   → Realistic products, prices, locations              │
│   → Simulates flash sales every 100 orders             │
│   → Simulates fraud bursts every 200 orders            │
│   → Publishes to Google Cloud Pub/Sub                  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              GOOGLE CLOUD PUB/SUB                       │
│   Topic        : ecommerce-orders                      │
│   Subscription : ecommerce-orders-sub                  │
│   → Buffers messages reliably                          │
│   → Delivers to stream processor                       │
│   → Handles backpressure automatically                 │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              STREAM PROCESSOR                           │
│   → Reads messages from Pub/Sub in real-time           │
│   → Applies 4 fraud detection rules:                   │
│      Rule 1: Pre-flagged by simulator                  │
│      Rule 2: Quantity >= 8 items                       │
│      Rule 3: Amount > $4,000                           │
│      Rule 4: 3+ orders in 60 seconds                   │
│   → Saves to BigQuery in batches of 5                  │
│   → Saves fraud events to fraud table                  │
│   → Tracks 1-minute metrics windows                    │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                    BIGQUERY                             │
│                                                         │
│   streaming_orders    → All live order events          │
│   realtime_metrics    → 1-minute aggregates            │
│   fraud_detection     → Fraud event log                │
│   anomaly_events      → AI-detected anomalies          │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│           GEMINI AI ANOMALY DETECTOR                    │
│   → Runs every 60 seconds                              │
│   → Collects metrics from last 5 minutes               │
│   → Compares to historical averages                    │
│   → Sends to Gemini AI for analysis                    │
│   → Detects: fraud spikes, revenue surges,             │
│              order anomalies, bot activity             │
│   → Saves all anomalies to BigQuery                    │
│   → Falls back to rule-based if Gemini unavailable     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              DAILY REPORT (GitHub Artifact)             │
│   → Pipeline summary                                   │
│   → Top 5 products by revenue                         │
│   → Fraud breakdown by type                            │
│   → All anomalies detected                             │
│   → Saved for 30 days                                  │
└─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
realtime-ecommerce-platform/
│
├── simulator/
│   └── order_simulator.py      # Publishes orders to Pub/Sub
│
├── pipeline/
│   └── stream_processor.py     # Reads Pub/Sub → BigQuery
│
├── detector/
│   └── anomaly_detector.py     # Gemini AI anomaly monitoring
│
├── infra/
│   ├── create_tables.py        # Creates BigQuery tables
│   └── daily_summary.py        # Generates daily report
│
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml  # GitHub Actions 6 AM daily
│
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variables template
└── README.md
```

---

## BigQuery Data Model

```
realtime_ecommerce.streaming_orders
├── order_id           STRING     Unique order identifier
├── customer_id        STRING     Customer identifier
├── product_name       STRING     Product name
├── category           STRING     Product category
├── quantity           INTEGER    Units ordered
├── unit_price         FLOAT      Price per unit
├── total_amount       FLOAT      Total order value
├── sales_channel      STRING     Website/Mobile/Marketplace
├── customer_location  STRING     City
├── device_type        STRING     iPhone/Android/Desktop
├── payment_method     STRING     Credit Card/PayPal/etc
├── status             STRING     completed/shipped/pending
├── is_fraud           BOOLEAN    Fraud flag
├── event_timestamp    TIMESTAMP  When order was placed
└── processed_at       TIMESTAMP  When saved to BigQuery

realtime_ecommerce.fraud_detection
├── fraud_id           STRING     Unique fraud event ID
├── order_id           STRING     Related order
├── customer_id        STRING     Fraudulent customer
├── fraud_type         STRING     Rule that triggered
├── confidence_score   FLOAT      0.0 to 0.95
├── total_amount       FLOAT      Fraudulent amount
└── flagged_at         TIMESTAMP  Detection time

realtime_ecommerce.anomaly_events
├── anomaly_id         STRING     Unique anomaly ID
├── anomaly_type       STRING     Type of anomaly
├── severity           STRING     CRITICAL/WARNING/INFO
├── description        STRING     What was detected
├── metric_value       FLOAT      Observed value
├── expected_value     FLOAT      Normal value
├── recommendation     STRING     Action to take
└── detected_at        TIMESTAMP  When detected

realtime_ecommerce.realtime_metrics
├── window_start       TIMESTAMP  Minute window start
├── window_end         TIMESTAMP  Minute window end
├── total_orders       INTEGER    Orders in window
├── total_revenue      FLOAT      Revenue in window
├── avg_order_value    FLOAT      Average order
├── top_product        STRING     Best selling product
├── top_channel        STRING     Best channel
└── fraud_count        INTEGER    Fraud in window
```

---

## Fraud Detection Rules

```
Rule 1 — Simulator Flagged
  → Order pre-marked as fraudulent
  → Confidence: 0.50

Rule 2 — High Quantity
  → 8 or more units in single order
  → Confidence: 0.65

Rule 3 — High Amount
  → Order value exceeds $4,000
  → Confidence: 0.65

Rule 4 — Rapid Orders
  → Same customer places 3+ orders in 60 seconds
  → Confidence: 0.80

Combined Rules → Higher Confidence:
  → 2 rules: 0.80
  → 3 rules: 0.95
```

---

## Daily Automation Flow

```
6:00 AM UTC — GitHub Actions triggers
      ↓
Install Python dependencies
      ↓
Set up GCP credentials
      ↓
Stream processor starts (background)
      ↓
Order simulator runs for 5 minutes
→ ~300 orders published to Pub/Sub
→ Processor reads and saves to BigQuery
→ Fraud detection runs on every order
      ↓
Wait 30 seconds for processing
      ↓
Anomaly detector runs 2 checks
→ Gemini AI analyzes metrics
→ Anomalies saved to BigQuery
      ↓
Daily summary generated
→ Orders, revenue, fraud, anomalies
→ Saved as GitHub artifact (30 days)
      ↓
Mac was completely OFF! ✅
```

---

## Sample Daily Report Output

```
REAL-TIME ECOMMERCE PIPELINE DAILY REPORT
Generated : 2026-05-28 06:00:11 UTC

PIPELINE SUMMARY (Last 1 Hour):
  Total orders   : 656
  Total revenue  : $815,495.65
  Avg order value: $1,243.13
  Fraud detected : 50
  Cities covered : 9
  Products sold  : 19

TOP 5 PRODUCTS:
  1. Peloton Bike           | 43 orders | $153,170.00
  2. iPhone 15 Pro          | 30 orders | $101,999.15
  3. Herman Miller Chair    | 35 orders |  $98,000.00
  4. Standing Desk          | 78 orders |  $92,845.00
  5. Dell XPS 15            | 41 orders |  $72,999.27

FRAUD SUMMARY:
  high_amount              | 21 cases
  flagged, high_qty        |  9 cases
  flagged                  |  4 cases

ANOMALIES DETECTED:
  [CRITICAL] FRAUD_SPIKE: 23% of revenue fraudulent
  [CRITICAL] ORDER_SURGE: 1150% above normal
  [CRITICAL] REVENUE_SURGE: 1100% above normal
  [WARNING]  LOW_AVG_ORDER: 20% below normal

PIPELINE STATUS: HEALTHY
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Event streaming | Google Cloud Pub/Sub | Message queue |
| Stream processing | Python | Order processing |
| Data warehouse | BigQuery | Storage + queries |
| AI monitoring | Google Gemini API | Anomaly detection |
| Fraud detection | Rule-based engine | Real-time flagging |
| Automation | GitHub Actions | Daily scheduling |
| Language | Python 3.10 | All scripts |
| Version control | GitHub | Code management |

---

## Setup Instructions

```bash
# 1. Clone repository
git clone https://github.com/Jeevan1122/realtime-ecommerce-platform.git
cd realtime-ecommerce-platform

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service_account_key.json"
export GCP_PROJECT_ID="your-gcp-project-id"
export GEMINI_API_KEY="your-gemini-api-key"

# 4. Create BigQuery tables
python infra/create_tables.py

# 5. Start stream processor (Terminal 1)
python pipeline/stream_processor.py

# 6. Start order simulator (Terminal 2)
python simulator/order_simulator.py

# 7. Start anomaly detector (Terminal 3)
python detector/anomaly_detector.py
```

---

## GitHub Actions Setup

```
Required secrets in repository settings:
→ GCP_KEY        : Service account JSON (plain text)
→ GEMINI_API_KEY : Google Gemini API key

Workflow schedule:
→ daily_pipeline.yml runs at 6 AM UTC daily
→ Also triggerable manually via workflow_dispatch

What the workflow does:
1. Installs all Python dependencies
2. Sets up GCP credentials from secrets
3. Starts stream processor in background
4. Runs simulator for 5 minutes (~300 orders)
5. Runs anomaly detector (2 checks)
6. Generates daily summary report
7. Saves report as GitHub artifact (30 days)
```

---

## Skills Demonstrated

**Real-Time Streaming**
- Google Cloud Pub/Sub topic and subscription management
- Event-driven architecture design
- Real-time message processing at scale
- Backpressure handling and batch optimization

**Data Engineering**
- BigQuery streaming inserts
- Multi-table data model design
- Real-time aggregations and windowing
- Schema design for time-series data

**AI and Machine Learning**
- Google Gemini API integration
- LLM-powered anomaly detection
- Real-time pattern recognition
- Intelligent alerting system

**Fraud Detection**
- Rule-based fraud detection engine
- Confidence scoring system
- Customer behavior analysis
- Real-time flagging pipeline

**DevOps and Cloud**
- GitHub Actions CI/CD
- GCP IAM and service accounts
- Scheduled workflow automation
- Secrets and credentials management

---

## Author

**Kodamati Jeevan Sai**
Senior Data Engineer and Team Lead
GCP Certified Associate Cloud Engineer
AWS Certified Data Engineer Associate

- LinkedIn: https://www.linkedin.com/in/kodamati-jeevan-sai-4b5390195
- GitHub: https://github.com/Jeevan1122

---

*Built with GCP · Pub/Sub · BigQuery · Gemini AI · GitHub Actions · Python*
