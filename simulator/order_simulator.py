import os
import json
import time
import random
import uuid
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")
TOPIC_ID   = os.getenv("PUBSUB_TOPIC", "ecommerce-orders")
publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

PRODUCTS = {
    "Electronics": [
        ("iPhone 15 Pro",      1199.99),
        ("Samsung Galaxy S24",  999.99),
        ("MacBook Air M2",     1299.99),
        ("Dell XPS 15",         999.99),
        ("Sony Headphones",     349.99),
        ("iPad Pro",            799.99),
    ],
    "Clothing": [
        ("Nike Air Max",        150.00),
        ("Adidas Ultraboost",   180.00),
        ("Levis Jeans",          60.00),
        ("North Face Jacket",   250.00),
        ("Puma Running Shoes",  120.00),
    ],
    "Furniture": [
        ("IKEA KALLAX",         169.00),
        ("Herman Miller Chair",1400.00),
        ("Standing Desk",       599.00),
        ("IKEA MALM Bed",       299.00),
    ],
    "Sports": [
        ("Peloton Bike",       1445.00),
        ("Yoga Mat",             79.00),
        ("Dumbbell Set",        149.00),
        ("Running Watch",       299.00),
    ],
}

CHANNELS  = ["Website", "Mobile App", "Marketplace", "Email", "Social"]
LOCATIONS = ["New York", "Los Angeles", "Chicago", "Houston",
             "Seattle", "Boston", "Atlanta", "Dallas", "Miami"]
DEVICES   = ["iPhone", "Android", "Desktop", "iPad", "MacBook"]
PAYMENTS  = ["Credit Card", "PayPal", "Apple Pay", "Google Pay"]
STATUSES  = ["completed", "processing", "shipped", "pending"]

def generate_order(fraud=False, customer_id=None):
    category    = random.choice(list(PRODUCTS.keys()))
    name, price = random.choice(PRODUCTS[category])
    quantity    = random.randint(5, 15) if fraud else random.randint(1, 3)
    return {
        "order_id"         : str(uuid.uuid4()),
        "customer_id"      : customer_id or f"CUST_{random.randint(1000,9999)}",
        "product_name"     : name,
        "category"         : category,
        "quantity"         : quantity,
        "unit_price"       : price,
        "total_amount"     : round(price * quantity, 2),
        "sales_channel"    : random.choice(CHANNELS),
        "customer_location": random.choice(LOCATIONS),
        "device_type"      : random.choice(DEVICES),
        "payment_method"   : random.choice(PAYMENTS),
        "status"           : random.choice(STATUSES),
        "is_fraud"         : fraud,
        "event_timestamp"  : datetime.utcnow().isoformat(),
        "processed_at"     : datetime.utcnow().isoformat(),
    }

def publish(order):
    try:
        msg = json.dumps(order).encode("utf-8")
        publisher.publish(topic_path, msg)
    except Exception as e:
        print(f"  ⚠️ Error: {e}", flush=True)

def main():
    print("=" * 55, flush=True)
    print("  REAL-TIME ORDER SIMULATOR", flush=True)
    print(f"  Project : {PROJECT_ID}", flush=True)
    print(f"  Topic   : {TOPIC_ID}", flush=True)
    print("=" * 55, flush=True)
    print(flush=True)

    count = fraud = 0
    revenue = 0.0

    try:
        while True:
            is_fraud = random.random() < 0.05
            order    = generate_order(fraud=is_fraud)
            count   += 1
            revenue += order["total_amount"]

            if is_fraud:
                fraud += 1
                print(f"  🚨 #{count:5d} FRAUD "
                      f"| {order['product_name'][:18]:18s} "
                      f"| ${order['total_amount']:8.2f} "
                      f"| {order['customer_location']}",
                      flush=True)
            else:
                print(f"  ✅ #{count:5d} Order "
                      f"| {order['product_name'][:18]:18s} "
                      f"| ${order['total_amount']:8.2f} "
                      f"| {order['customer_location']}",
                      flush=True)

            publish(order)

            if count % 100 == 0:
                print(flush=True)
                print("  ⚡ FLASH SALE TRIGGERED!", flush=True)
                for _ in range(20):
                    fo = generate_order()
                    count += 1
                    revenue += fo["total_amount"]
                    print(f"  ⚡ #{count:5d} Flash "
                          f"| {fo['product_name'][:18]:18s} "
                          f"| ${fo['total_amount']:8.2f}",
                          flush=True)
                    publish(fo)
                    time.sleep(0.05)
                print(flush=True)

            if count % 50 == 0:
                print(flush=True)
                print(f"  📊 {count} orders | "
                      f"${revenue:,.2f} | {fraud} fraud",
                      flush=True)
                print(flush=True)

            time.sleep(0.8)

    except KeyboardInterrupt:
        print(flush=True)
        print("=" * 55, flush=True)
        print(f"  Orders  : {count}", flush=True)
        print(f"  Revenue : ${revenue:,.2f}", flush=True)
        print(f"  Fraud   : {fraud}", flush=True)
        print("=" * 55, flush=True)

if __name__ == "__main__":
    main()
