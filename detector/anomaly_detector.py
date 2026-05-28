"""
Real-Time AI Anomaly Detector
Uses Gemini AI to analyze pipeline metrics
and detect anomalies every 60 seconds
"""
import os
import json
import time
import uuid
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID     = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATASET        = "realtime_ecommerce"
BQ             = bigquery.Client(project=PROJECT_ID)

ANOMALY_TABLE  = f"{PROJECT_ID}.{DATASET}.anomaly_events"
CHECK_INTERVAL = 60

def ask_gemini(prompt):
    url     = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        response = requests.post(url, json=payload, timeout=30)
        data     = response.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        return None

def get_current_metrics():
    """Get metrics from last 5 minutes"""
    now      = datetime.now(timezone.utc)
    five_ago = (now - timedelta(minutes=5)).isoformat()

    result = BQ.query(f"""
        SELECT
            COUNT(*)                           AS orders_5min,
            ROUND(SUM(total_amount), 2)        AS revenue_5min,
            ROUND(AVG(total_amount), 2)        AS avg_order_value,
            COUNTIF(is_fraud = true)           AS fraud_5min,
            COUNT(DISTINCT customer_location)  AS unique_cities,
            COUNT(DISTINCT product_name)       AS unique_products,
            COUNT(DISTINCT sales_channel)      AS active_channels
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{five_ago}'
    """).result()

    for row in result:
        return dict(row)
    return {}

def get_historical_metrics():
    """Get average metrics from last hour for comparison"""
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    five_ago = (now - timedelta(minutes=5)).isoformat()

    result = BQ.query(f"""
        SELECT
            COUNT(*)                    AS total_orders,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        AND   processed_at < '{five_ago}'
    """).result()

    for row in result:
        return dict(row)
    return {}

def get_top_products():
    """Get top 3 products by revenue in last 5 minutes"""
    now      = datetime.now(timezone.utc)
    five_ago = (now - timedelta(minutes=5)).isoformat()

    result = BQ.query(f"""
        SELECT
            product_name,
            COUNT(*)                    AS orders,
            ROUND(SUM(total_amount), 2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{five_ago}'
        GROUP BY product_name
        ORDER BY revenue DESC
        LIMIT 3
    """).result()

    return [dict(row) for row in result]

def get_fraud_summary():
    """Get fraud summary from last 5 minutes"""
    now      = datetime.now(timezone.utc)
    five_ago = (now - timedelta(minutes=5)).isoformat()

    result = BQ.query(f"""
        SELECT
            fraud_type,
            COUNT(*)                    AS count,
            ROUND(SUM(total_amount), 2) AS amount
        FROM `{PROJECT_ID}.{DATASET}.fraud_detection`
        WHERE flagged_at >= '{five_ago}'
        GROUP BY fraud_type
        ORDER BY count DESC
    """).result()

    return [dict(row) for row in result]

def save_anomaly(anomaly_type, severity,
                 description, metric_val,
                 expected_val, recommendation):
    try:
        BQ.insert_rows_json(ANOMALY_TABLE, [{
            "anomaly_id"    : str(uuid.uuid4()),
            "anomaly_type"  : anomaly_type,
            "severity"      : severity,
            "description"   : description,
            "metric_value"  : float(metric_val),
            "expected_value": float(expected_val),
            "recommendation": recommendation,
            "detected_at"   : datetime.now(timezone.utc).isoformat(),
        }])
        print(f"  💾 Anomaly saved: {anomaly_type}", flush=True)
    except Exception as e:
        print(f"  ⚠️ Save error: {e}", flush=True)

def rule_based_check(current, historical, fraud_summary):
    """Check for anomalies using rules"""
    anomalies = []

    # Check 1: Zero orders in 5 minutes
    if current.get("orders_5min", 0) == 0:
        anomalies.append({
            "type"          : "NO_ORDERS",
            "severity"      : "CRITICAL",
            "description"   : "No orders in last 5 minutes!",
            "metric_value"  : 0,
            "expected_value": 10,
            "recommendation": "Check simulator and pipeline"
        })

    # Check 2: Revenue spike
    hist_avg = historical.get("avg_order_value", 0)
    curr_avg = current.get("avg_order_value", 0)
    if hist_avg > 0 and curr_avg > hist_avg * 3:
        anomalies.append({
            "type"          : "REVENUE_SPIKE",
            "severity"      : "WARNING",
            "description"   : f"Avg order ${curr_avg:.2f} vs normal ${hist_avg:.2f}",
            "metric_value"  : curr_avg,
            "expected_value": hist_avg,
            "recommendation": "Verify pricing or flash sale"
        })

    # Check 3: High fraud rate
    fraud_count = current.get("fraud_5min", 0)
    total       = current.get("orders_5min", 1)
    fraud_rate  = fraud_count / total if total > 0 else 0
    if fraud_rate > 0.15:
        anomalies.append({
            "type"          : "HIGH_FRAUD_RATE",
            "severity"      : "CRITICAL",
            "description"   : f"Fraud rate {fraud_rate:.1%} ({fraud_count} of {total})",
            "metric_value"  : fraud_rate,
            "expected_value": 0.05,
            "recommendation": "Review fraud patterns immediately"
        })

    return anomalies

def analyze_with_gemini(current, historical,
                        top_products, fraud_summary):
    """Use Gemini AI for intelligent analysis"""
    prompt = f"""
You are a real-time ecommerce data analyst.
Analyze these metrics and detect anomalies.

TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

LAST 5 MINUTES:
- Orders      : {current.get('orders_5min', 0)}
- Revenue     : ${current.get('revenue_5min', 0):,.2f}
- Avg order   : ${current.get('avg_order_value', 0):,.2f}
- Fraud events: {current.get('fraud_5min', 0)}
- Active cities: {current.get('unique_cities', 0)}

HISTORICAL AVERAGE (last hour):
- Total orders: {historical.get('total_orders', 0)}
- Avg order   : ${historical.get('avg_order_value', 0):,.2f}
- Total revenue: ${historical.get('total_revenue', 0):,.2f}

TOP PRODUCTS:
{json.dumps(top_products, indent=2, default=str)}

FRAUD SUMMARY:
{json.dumps(fraud_summary, indent=2, default=str)}

Respond ONLY in this JSON format:
{{
  "status": "HEALTHY/WARNING/CRITICAL",
  "anomalies": [
    {{
      "type": "ANOMALY_TYPE",
      "severity": "CRITICAL/WARNING/INFO",
      "description": "What was detected",
      "recommendation": "What to do"
    }}
  ],
  "summary": "2 sentence plain English summary"
}}
"""
    response = ask_gemini(prompt)
    if not response:
        return None

    try:
        start  = response.find("{")
        end    = response.rfind("}") + 1
        if start != -1 and end > start:
            return json.loads(response[start:end])
    except Exception:
        pass
    return None

def run_check():
    """Run one anomaly detection cycle"""
    print(flush=True)
    print(f"  🔍 Checking at "
          f"{datetime.now().strftime('%H:%M:%S')}...",
          flush=True)

    current     = get_current_metrics()
    historical  = get_historical_metrics()
    top_prods   = get_top_products()
    fraud_sum   = get_fraud_summary()

    if not current:
        print("  ⚠️ No metrics available yet", flush=True)
        return

    print(f"  📊 Last 5min: "
          f"{current.get('orders_5min',0)} orders | "
          f"${current.get('revenue_5min',0):,.2f} | "
          f"{current.get('fraud_5min',0)} fraud",
          flush=True)

    # Rule-based checks
    rule_anomalies = rule_based_check(
        current, historical, fraud_sum
    )

    # Gemini AI analysis
    print("  🤖 Asking Gemini AI...", flush=True)
    ai_result = analyze_with_gemini(
        current, historical, top_prods, fraud_sum
    )

    if ai_result:
        status = ai_result.get("status", "UNKNOWN")
        print(f"  🎯 Status : {status}", flush=True)
        print(f"  💬 {ai_result.get('summary', '')}", flush=True)

        for anomaly in ai_result.get("anomalies", []):
            sev  = anomaly.get("severity", "INFO")
            atype = anomaly.get("type", "UNKNOWN")
            desc = anomaly.get("description", "")
            rec  = anomaly.get("recommendation", "")

            icon = "🔴" if sev == "CRITICAL" \
                   else "🟡" if sev == "WARNING" \
                   else "🟢"
            print(f"  {icon} {atype}: {desc}", flush=True)
            print(f"     → {rec}", flush=True)

            save_anomaly(
                atype, sev, desc,
                current.get("orders_5min", 0),
                historical.get("total_orders", 0) / 12,
                rec
            )
    else:
        print("  ⚠️ Gemini unavailable — using rules",
              flush=True)
        for a in rule_anomalies:
            icon = "🔴" if a["severity"] == "CRITICAL" \
                   else "🟡"
            print(f"  {icon} {a['type']}: {a['description']}",
                  flush=True)
            save_anomaly(
                a["type"], a["severity"],
                a["description"],
                a["metric_value"],
                a["expected_value"],
                a["recommendation"]
            )

        if not rule_anomalies:
            print("  🟢 All metrics normal!", flush=True)

def main():
    print("=" * 55, flush=True)
    print("  REAL-TIME AI ANOMALY DETECTOR", flush=True)
    print(f"  Project  : {PROJECT_ID}", flush=True)
    print(f"  Interval : {CHECK_INTERVAL}s", flush=True)
    print(f"  AI Model : Gemini 2.5 Flash", flush=True)
    print("=" * 55, flush=True)
    print(flush=True)
    print("  Monitoring pipeline... Ctrl+C to stop", flush=True)

    check_count = 0
    try:
        while True:
            check_count += 1
            print(flush=True)
            print(f"  {'='*50}", flush=True)
            print(f"  CHECK #{check_count}", flush=True)
            run_check()
            print(flush=True)
            print(f"  Next check in {CHECK_INTERVAL}s...",
                  flush=True)
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print(flush=True)
        print("=" * 55, flush=True)
        print(f"  Detector stopped after {check_count} checks",
              flush=True)
        print("=" * 55, flush=True)

if __name__ == "__main__":
    main()
