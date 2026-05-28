from google.cloud import bigquery
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")
DATASET    = "realtime_ecommerce"
client     = bigquery.Client(project=PROJECT_ID)

TABLES = {
    "streaming_orders": [
        bigquery.SchemaField("order_id",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_id",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("product_name",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("category",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("quantity",           "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("unit_price",         "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("total_amount",       "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("sales_channel",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_location",  "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("device_type",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("payment_method",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("status",             "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("is_fraud",           "BOOLEAN",   mode="REQUIRED"),
        bigquery.SchemaField("event_timestamp",    "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("processed_at",       "TIMESTAMP", mode="REQUIRED"),
    ],
    "realtime_metrics": [
        bigquery.SchemaField("window_start",    "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("window_end",      "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("total_orders",    "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("total_revenue",   "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("avg_order_value", "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("top_product",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("top_channel",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("fraud_count",     "INTEGER",   mode="REQUIRED"),
    ],
    "anomaly_events": [
        bigquery.SchemaField("anomaly_id",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("anomaly_type",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("severity",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("description",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("metric_value",   "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("expected_value", "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("recommendation", "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("detected_at",    "TIMESTAMP", mode="REQUIRED"),
    ],
    "fraud_detection": [
        bigquery.SchemaField("fraud_id",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("order_id",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("fraud_type",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("confidence_score", "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("total_amount",     "FLOAT",     mode="REQUIRED"),
        bigquery.SchemaField("flagged_at",       "TIMESTAMP", mode="REQUIRED"),
    ],
}

print("Creating BigQuery tables...")
print()
for table_name, schema in TABLES.items():
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    table    = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table)
        print(f"  ✅ Created : {table_name}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"  ✅ Exists  : {table_name}")
        else:
            print(f"  ❌ Error   : {table_name} — {e}")
print()
print("✅ All tables ready!")
