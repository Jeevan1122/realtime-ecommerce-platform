
"""

Daily Pipeline Summary

Generates report after pipeline runs

"""

import os

from datetime import datetime, timezone, timedelta

from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")

DATASET    = "realtime_ecommerce"

BQ         = bigquery.Client(project=PROJECT_ID)

def query(sql):

    try:

        rows = BQ.query(sql).result()

        return [dict(row) for row in rows]

    except Exception as e:

        return [{"error": str(e)}]

def main():

    now      = datetime.now(timezone.utc)

    hour_ago = (now - timedelta(hours=1)).isoformat()

    print("Generating daily summary...")

    orders = query(f"""

        SELECT

            COUNT(*)                           AS total_orders,

            ROUND(SUM(total_amount), 2)        AS total_revenue,

            ROUND(AVG(total_amount), 2)        AS avg_order,

            COUNTIF(is_fraud = true)           AS fraud_count,

            COUNT(DISTINCT customer_location)  AS cities,

            COUNT(DISTINCT product_name)       AS products

        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`

        WHERE processed_at >= '{hour_ago}'

    """)

    top_products = query(f"""

        SELECT product_name,

               COUNT(*) AS orders,

               ROUND(SUM(total_amount),2) AS revenue

        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`

        WHERE processed_at >= '{hour_ago}'

        GROUP BY product_name

        ORDER BY revenue DESC

        LIMIT 5

    """)

    fraud = query(f"""

        SELECT fraud_type, COUNT(*) AS count

        FROM `{PROJECT_ID}.{DATASET}.fraud_detection`

        WHERE flagged_at >= '{hour_ago}'

        GROUP BY fraud_type

        ORDER BY count DESC

    """)

    anomalies = query(f"""

        SELECT anomaly_type, severity, description

        FROM `{PROJECT_ID}.{DATASET}.anomaly_events`

        WHERE detected_at >= '{hour_ago}'

        ORDER BY detected_at DESC

        LIMIT 10

    """)

    o = orders[0] if orders else {}

    report = f"""

{'='*55}

REAL-TIME ECOMMERCE PIPELINE DAILY REPORT

Generated : {now.strftime('%Y-%m-%d %H:%M:%S')} UTC

Project   : {PROJECT_ID}

{'='*55}

PIPELINE SUMMARY (Last 1 Hour):

  Total orders   : {o.get('total_orders', 0)}

  Total revenue  : ${o.get('total_revenue', 0):,.2f}

  Avg order value: ${o.get('avg_order', 0):,.2f}

  Fraud detected : {o.get('fraud_count', 0)}

  Cities covered : {o.get('cities', 0)}

  Products sold  : {o.get('products', 0)}

TOP 5 PRODUCTS:

{''.join(f"  {i+1}. {p.get('product_name','N/A'):25s} | {p.get('orders',0):4d} orders | ${p.get('revenue',0):10,.2f}" + chr(10) for i, p in enumerate(top_products))}

FRAUD SUMMARY:

{''.join(f"  {f.get('fraud_type','N/A'):20s} | {f.get('count',0)} cases" + chr(10) for f in fraud) if fraud else "  No fraud detected"}

ANOMALIES DETECTED:

{''.join(f"  [{a.get('severity','N/A')}] {a.get('anomaly_type','N/A')}: {a.get('description','N/A')}" + chr(10) for a in anomalies) if anomalies else "  No anomalies detected"}

PIPELINE STATUS: {"✅ HEALTHY" if o.get('total_orders', 0) > 0 else "❌ NO DATA"}

{'='*55}

"""

    os.makedirs("reports", exist_ok=True)

    filename = f"reports/daily_report_{now.strftime('%Y-%m-%d')}.txt"

    with open(filename, "w") as f:

        f.write(report)

    print(report)

    print(f"Report saved: {filename}")

if __name__ == "__main__":

    main()

