import os
import json
import uuid
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.cloud import bigquery

load_dotenv()

PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")
SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "ecommerce-orders-sub")
DATASET      = os.getenv("BIGQUERY_DATASET", "realtime_ecommerce")
BQ           = bigquery.Client(project=PROJECT_ID)
SUB_CLIENT   = pubsub_v1.SubscriberClient()
SUB_PATH     = SUB_CLIENT.subscription_path(PROJECT_ID, SUBSCRIPTION)

ORDERS_TABLE  = f"{PROJECT_ID}.{DATASET}.streaming_orders"
FRAUD_TABLE   = f"{PROJECT_ID}.{DATASET}.fraud_detection"
METRICS_TABLE = f"{PROJECT_ID}.{DATASET}.realtime_metrics"

batch            = []
metrics_batch    = []
customer_history = defaultdict(list)
BATCH_SIZE       = 5

stats = {
    "received": 0, "saved": 0,
    "fraud": 0, "revenue": 0.0, "errors": 0
}

def detect_fraud(order):
    reasons = []
    cid     = order["customer_id"]
    now     = datetime.now(timezone.utc)

    if order.get("is_fraud", False):
        reasons.append("flagged")
    if int(order["quantity"]) >= 8:
        reasons.append("high_qty")
    if float(order["total_amount"]) > 4000:
        reasons.append("high_amount")

    recent = [t for t in customer_history[cid]
              if (now - t).total_seconds() < 60]
    if len(recent) >= 3:
        reasons.append("rapid_orders")

    customer_history[cid].append(now)
    if len(customer_history[cid]) > 50:
        customer_history[cid] = customer_history[cid][-50:]

    return reasons

def flush_batch():
    global batch
    if not batch:
        return

    rows = []
    for o in batch:
        try:
            rows.append({
                "order_id"         : str(o["order_id"]),
                "customer_id"      : str(o["customer_id"]),
                "product_name"     : str(o["product_name"]),
                "category"         : str(o["category"]),
                "quantity"         : int(o["quantity"]),
                "unit_price"       : float(o["unit_price"]),
                "total_amount"     : float(o["total_amount"]),
                "sales_channel"    : str(o["sales_channel"]),
                "customer_location": str(o["customer_location"]),
                "device_type"      : str(o["device_type"]),
                "payment_method"   : str(o["payment_method"]),
                "status"           : str(o["status"]),
                "is_fraud"         : bool(o.get("is_fraud", False)),
                "event_timestamp"  : str(o.get("event_timestamp",
                    datetime.now(timezone.utc).isoformat())),
                "processed_at"     : datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"  ⚠️ Row error: {e}", flush=True)

    try:
        errors = BQ.insert_rows_json(
            ORDERS_TABLE, rows,
            skip_invalid_rows=True,
            ignore_unknown_values=True
        )
        if errors:
            print(f"  ⚠️ BQ error: {errors[0]}", flush=True)
            stats["errors"] += 1
        else:
            stats["saved"] += len(rows)
            print(f"  💾 Saved {len(rows)} | Total BQ: {stats['saved']}",
                  flush=True)
    except Exception as e:
        print(f"  ❌ BQ error: {e}", flush=True)
        stats["errors"] += 1

    batch = []

def save_fraud(order, reasons):
    try:
        BQ.insert_rows_json(FRAUD_TABLE, [{
            "fraud_id"        : str(uuid.uuid4()),
            "order_id"        : str(order["order_id"]),
            "customer_id"     : str(order["customer_id"]),
            "fraud_type"      : ", ".join(reasons),
            "confidence_score": round(min(0.95, 0.5+len(reasons)*0.15), 2),
            "total_amount"    : float(order["total_amount"]),
            "flagged_at"      : datetime.now(timezone.utc).isoformat(),
        }])
    except Exception as e:
        print(f"  ⚠️ Fraud error: {e}", flush=True)

def save_metrics():
    if not metrics_batch:
        return
    try:
        total   = len(metrics_batch)
        rev     = sum(float(o["total_amount"]) for o in metrics_batch)
        fraud_c = sum(1 for o in metrics_batch if o.get("is_fraud"))
        prods   = defaultdict(float)
        chans   = defaultdict(int)
        for o in metrics_batch:
            prods[o["product_name"]] += float(o["total_amount"])
            chans[o["sales_channel"]] += 1
        now = datetime.now(timezone.utc).isoformat()
        BQ.insert_rows_json(METRICS_TABLE, [{
            "window_start"   : now,
            "window_end"     : now,
            "total_orders"   : total,
            "total_revenue"  : round(rev, 2),
            "avg_order_value": round(rev/total, 2),
            "top_product"    : max(prods, key=prods.get) if prods else "N/A",
            "top_channel"    : max(chans, key=chans.get) if chans else "N/A",
            "fraud_count"    : fraud_c,
        }])
        print(f"  📊 Metrics: {total} orders | ${rev:,.2f}",
              flush=True)
    except Exception as e:
        print(f"  ⚠️ Metrics error: {e}", flush=True)
    metrics_batch.clear()

def callback(message):
    global batch, metrics_batch
    try:
        order = json.loads(message.data.decode("utf-8"))
        stats["received"] += 1
        stats["revenue"]  += float(order["total_amount"])

        reasons = detect_fraud(order)
        if reasons:
            stats["fraud"] += 1
            order["is_fraud"] = True
            save_fraud(order, reasons)
            print(f"  🚨 #{stats['received']:5d} FRAUD "
                  f"| {order['product_name'][:18]:18s} "
                  f"| ${order['total_amount']:8.2f} "
                  f"| {', '.join(reasons)}", flush=True)
        else:
            print(f"  ✅ #{stats['received']:5d} Order "
                  f"| {order['product_name'][:18]:18s} "
                  f"| ${order['total_amount']:8.2f} "
                  f"| {order['sales_channel']}", flush=True)

        batch.append(order)
        metrics_batch.append(order)

        if len(batch) >= BATCH_SIZE:
            flush_batch()
        if len(metrics_batch) >= 20:
            save_metrics()

        if stats["received"] % 50 == 0:
            print(flush=True)
            print(f"  📈 received={stats['received']} "
                  f"saved={stats['saved']} "
                  f"fraud={stats['fraud']} "
                  f"revenue=${stats['revenue']:,.2f}", flush=True)
            print(flush=True)

        message.ack()
    except Exception as e:
        print(f"  ❌ Error: {e}", flush=True)
        message.nack()

def main():
    print("=" * 55, flush=True)
    print("  REAL-TIME STREAM PROCESSOR", flush=True)
    print(f"  Project      : {PROJECT_ID}", flush=True)
    print(f"  Subscription : {SUBSCRIPTION}", flush=True)
    print(f"  Batch size   : {BATCH_SIZE}", flush=True)
    print("=" * 55, flush=True)
    print(flush=True)
    print("  Waiting for orders... Ctrl+C to stop", flush=True)
    print(flush=True)

    streaming = SUB_CLIENT.subscribe(SUB_PATH, callback=callback)
    print(f"  ✅ Listening on: {SUB_PATH}", flush=True)
    print(flush=True)

    try:
        streaming.result(timeout=3600)
    except KeyboardInterrupt:
        streaming.cancel()
        streaming.result()
        flush_batch()
        save_metrics()
        print(flush=True)
        print("=" * 55, flush=True)
        print(f"  Received : {stats['received']}", flush=True)
        print(f"  Saved BQ : {stats['saved']}", flush=True)
        print(f"  Fraud    : {stats['fraud']}", flush=True)
        print(f"  Revenue  : ${stats['revenue']:,.2f}", flush=True)
        print("=" * 55, flush=True)
    except Exception as e:
        print(f"  ❌ Fatal: {e}", flush=True)
        streaming.cancel()

if __name__ == "__main__":
    main()
