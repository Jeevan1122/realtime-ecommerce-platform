"""
Real-Time E-Commerce Intelligence Dashboard
"""
import os
import json
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Real-Time E-Commerce Intelligence",
    page_icon="🛒",
    layout="wide"
)

st.markdown("""
<style>
.fraud-alert {
    background:#1A0000; border:1px solid #EF4444;
    border-radius:8px; padding:10px; margin:5px 0;
}
.anomaly-critical {
    background:#1A0000; border-left:4px solid #EF4444;
    padding:8px; margin:4px 0; border-radius:4px;
}
.anomaly-warning {
    background:#1A0E00; border-left:4px solid #F59E0B;
    padding:8px; margin:4px 0; border-radius:4px;
}
.healthy {
    background:#001A0A; border-left:4px solid #22C55E;
    padding:8px; margin:4px 0; border-radius:4px;
}
</style>
""", unsafe_allow_html=True)

PROJECT_ID = "ecommerce-dashboard-497321"
DATASET    = "realtime_ecommerce"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = {
                "type"                       : st.secrets["gcp_service_account"]["type"],
                "project_id"                 : st.secrets["gcp_service_account"]["project_id"],
                "private_key_id"             : st.secrets["gcp_service_account"]["private_key_id"],
                "private_key"                : st.secrets["gcp_service_account"]["private_key"],
                "client_email"               : st.secrets["gcp_service_account"]["client_email"],
                "client_id"                  : st.secrets["gcp_service_account"]["client_id"],
                "auth_uri"                   : st.secrets["gcp_service_account"]["auth_uri"],
                "token_uri"                  : st.secrets["gcp_service_account"]["token_uri"],
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url"       : f"https://www.googleapis.com/robot/v1/metadata/x509/dashboard-sa%40ecommerce-dashboard-497321.iam.gserviceaccount.com"
            }
            creds = service_account.Credentials.from_service_account_info(
                info,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return bigquery.Client(
                project    =PROJECT_ID,
                credentials=creds
            )
        else:
            st.error("❌ gcp_service_account not found in secrets!")
            return None
    except Exception as e:
        st.error(f"❌ BQ connection error: {e}")
        return None

def run_query(sql):
    try:
        client = get_bq_client()
        if client is None:
            return pd.DataFrame()
        return client.query(sql).to_dataframe()
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()

def get_summary_stats():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    min5_ago = (now - timedelta(minutes=5)).isoformat()
    df = run_query(f"""
        SELECT
            COUNT(*)                    AS total_orders,
            ROUND(SUM(total_amount),2)  AS total_revenue,
            ROUND(AVG(total_amount),2)  AS avg_order,
            COUNTIF(is_fraud = true)    AS fraud_count,
            COUNT(DISTINCT customer_location) AS cities,
            COUNT(DISTINCT product_name)      AS products,
            COUNTIF(processed_at >= '{min5_ago}') AS orders_5min,
            ROUND(SUM(CASE WHEN processed_at >= '{min5_ago}'
                THEN total_amount ELSE 0 END),2) AS revenue_5min
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
    """)
    return df.iloc[0] if not df.empty else {}

def get_orders_over_time():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    return run_query(f"""
        SELECT
            TIMESTAMP_TRUNC(processed_at, MINUTE) AS minute,
            COUNT(*) AS orders,
            ROUND(SUM(total_amount),2) AS revenue,
            COUNTIF(is_fraud=true) AS fraud
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY minute ORDER BY minute
    """)

def get_top_products():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    return run_query(f"""
        SELECT product_name,
               COUNT(*) AS orders,
               ROUND(SUM(total_amount),2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY product_name
        ORDER BY revenue DESC LIMIT 8
    """)

def get_channel_breakdown():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    return run_query(f"""
        SELECT sales_channel,
               COUNT(*) AS orders,
               ROUND(SUM(total_amount),2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY sales_channel
        ORDER BY revenue DESC
    """)

def get_city_breakdown():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    return run_query(f"""
        SELECT customer_location AS city,
               COUNT(*) AS orders,
               ROUND(SUM(total_amount),2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY city ORDER BY orders DESC LIMIT 9
    """)

def get_recent_fraud():
    return run_query(f"""
        SELECT customer_id, fraud_type,
               ROUND(confidence_score*100,0) AS confidence_pct,
               ROUND(total_amount,2) AS amount,
               flagged_at
        FROM `{PROJECT_ID}.{DATASET}.fraud_detection`
        ORDER BY flagged_at DESC LIMIT 10
    """)

def get_recent_anomalies():
    return run_query(f"""
        SELECT anomaly_type, severity,
               description, recommendation,
               detected_at
        FROM `{PROJECT_ID}.{DATASET}.anomaly_events`
        ORDER BY detected_at DESC LIMIT 8
    """)

def get_live_orders():
    return run_query(f"""
        SELECT product_name, category,
               ROUND(total_amount,2) AS amount,
               sales_channel, customer_location,
               is_fraud, processed_at
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        ORDER BY processed_at DESC LIMIT 20
    """)

def main():
    st.markdown("""
    <div style='text-align:center; padding:20px 0 10px'>
        <h1 style='color:#F9FAFB; font-size:2rem; margin:0'>
            🛒 Real-Time E-Commerce Intelligence
        </h1>
        <p style='color:#6B7280; margin:5px 0'>
            Live pipeline · BigQuery · Gemini AI
        </p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("## ⚙️ Controls")
        refresh = st.slider("Refresh (sec)", 10, 120, 30)
        st.markdown("---")
        st.markdown("### 📊 Data Source")
        st.code(f"Project: {PROJECT_ID}\nDataset: {DATASET}")
        st.markdown("---")
        st.markdown("[📦 GitHub](https://github.com/Jeevan1122/realtime-ecommerce-platform)")
        st.markdown("---")
        st.caption(f"Last: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("🔄 Refresh"):
            st.cache_resource.clear()
            st.rerun()

    stats = get_summary_stats()
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    with c1:
        st.metric("Total Orders",
            f"{int(stats.get('total_orders',0)):,}",
            f"+{int(stats.get('orders_5min',0))} (5min)")
    with c2:
        st.metric("Revenue",
            f"${stats.get('total_revenue',0):,.0f}",
            f"+${stats.get('revenue_5min',0):,.0f} (5min)")
    with c3:
        st.metric("Avg Order",
            f"${stats.get('avg_order',0):,.2f}")
    with c4:
        fraud = int(stats.get('fraud_count',0))
        total = int(stats.get('total_orders',1))
        rate  = fraud/total*100 if total > 0 else 0
        st.metric("Fraud Events", f"{fraud}",
            f"{rate:.1f}% rate", delta_color="inverse")
    with c5:
        st.metric("Cities", f"{int(stats.get('cities',0))}")
    with c6:
        st.metric("Products", f"{int(stats.get('products',0))}")

    st.markdown("---")

    col1, col2 = st.columns([2,1])
    with col1:
        st.markdown("#### 📈 Orders & Fraud Over Time")
        df_time = get_orders_over_time()
        if not df_time.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_time["minute"], y=df_time["orders"],
                name="Orders", marker_color="#3B82F6", opacity=0.8))
            fig.add_trace(go.Scatter(
                x=df_time["minute"], y=df_time["fraud"],
                name="Fraud", mode="lines+markers",
                line=dict(color="#EF4444", width=2), yaxis="y2"))
            fig.update_layout(
                paper_bgcolor="#161B22", plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"), height=300,
                margin=dict(l=0,r=0,t=10,b=0),
                yaxis=dict(title="Orders"),
                yaxis2=dict(title="Fraud", overlaying="y",
                    side="right", color="#EF4444"),
                legend=dict(bgcolor="#161B22"))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet — run the pipeline first!")

    with col2:
        st.markdown("#### 📡 Sales Channels")
        df_chan = get_channel_breakdown()
        if not df_chan.empty:
            fig = px.pie(df_chan, values="revenue",
                names="sales_channel", hole=0.4,
                color_discrete_sequence=[
                    "#3B82F6","#8B5CF6","#14B8A6","#F59E0B","#EF4444"])
            fig.update_layout(
                paper_bgcolor="#161B22",
                font=dict(color="#9CA3AF"), height=300,
                margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet!")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("#### 🏆 Top Products")
        df_prod = get_top_products()
        if not df_prod.empty:
            fig = px.bar(df_prod, x="revenue", y="product_name",
                orientation="h", color="revenue",
                color_continuous_scale="Blues", text="orders")
            fig.update_layout(
                paper_bgcolor="#161B22", plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"), height=320,
                margin=dict(l=0,r=0,t=10,b=0),
                coloraxis_showscale=False,
                yaxis=dict(categoryorder="total ascending"))
            fig.update_traces(
                texttemplate="%{text} orders",
                textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet!")

    with col4:
        st.markdown("#### 🌆 Orders by City")
        df_city = get_city_breakdown()
        if not df_city.empty:
            fig = px.bar(df_city, x="city", y="orders",
                color="revenue", color_continuous_scale="Teal",
                text="orders")
            fig.update_layout(
                paper_bgcolor="#161B22", plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"), height=320,
                margin=dict(l=0,r=0,t=10,b=0),
                coloraxis_showscale=False)
            fig.update_traces(
                texttemplate="%{text}",
                textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet!")

    st.markdown("---")

    col5, col6 = st.columns(2)
    with col5:
        st.markdown("#### 🚨 Recent Fraud Events")
        df_fraud = get_recent_fraud()
        if not df_fraud.empty:
            for _, row in df_fraud.iterrows():
                conf = row.get("confidence_pct", 0)
                c    = "#EF4444" if conf >= 80 else "#F59E0B"
                st.markdown(f"""
                <div class='fraud-alert'>
                    <b style='color:{c}'>⚠️ {row['customer_id']}</b>
                    <span style='float:right;color:#9CA3AF'>
                        ${row['amount']:,.2f}</span><br>
                    <span style='color:#6B7280;font-size:0.8rem'>
                        {row['fraud_type']} · {int(conf)}% confidence
                    </span>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown(
                "<div class='healthy'>✅ No fraud detected!</div>",
                unsafe_allow_html=True)

    with col6:
        st.markdown("#### 🤖 AI Anomaly Alerts")
        df_anom = get_recent_anomalies()
        if not df_anom.empty:
            for _, row in df_anom.iterrows():
                sev  = row.get("severity","INFO")
                css  = "anomaly-critical" if sev=="CRITICAL" \
                       else "anomaly-warning"
                icon = "🔴" if sev=="CRITICAL" else "🟡"
                desc = str(row['description'])[:80]
                rec  = str(row['recommendation'])[:60]
                st.markdown(f"""
                <div class='{css}'>
                    <b style='color:#F9FAFB'>
                        {icon} {row['anomaly_type']}</b><br>
                    <span style='color:#9CA3AF;font-size:0.82rem'>
                        {desc}...</span><br>
                    <span style='color:#6B7280;font-size:0.75rem'>
                        → {rec}</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown(
                "<div class='healthy'>✅ Pipeline healthy!</div>",
                unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("#### ⚡ Live Order Feed")
    df_orders = get_live_orders()
    if not df_orders.empty:
        df_display = df_orders.copy()
        df_display["amount"] = df_display["amount"].apply(
            lambda x: f"${x:,.2f}")
        df_display["is_fraud"] = df_display["is_fraud"].apply(
            lambda x: "🚨 FRAUD" if x else "✅ OK")
        df_display["processed_at"] = pd.to_datetime(
            df_display["processed_at"]).dt.strftime("%H:%M:%S")
        df_display = df_display.rename(columns={
            "processed_at":"Time", "product_name":"Product",
            "amount":"Amount", "sales_channel":"Channel",
            "customer_location":"City", "is_fraud":"Status"})
        st.dataframe(df_display, use_container_width=True,
                     hide_index=True, height=400)
    else:
        st.info("No orders yet — start the pipeline!")

    st.markdown("---")
    st.markdown(f"""
    <div style='text-align:center;color:#4B5563;font-size:0.8rem'>
        Real-Time E-Commerce Intelligence · Kodamati Jeevan Sai ·
        BigQuery + Gemini AI · Refreshes every {refresh}s
    </div>""", unsafe_allow_html=True)

    time.sleep(refresh)
    st.rerun()

if __name__ == "__main__":
    main()
