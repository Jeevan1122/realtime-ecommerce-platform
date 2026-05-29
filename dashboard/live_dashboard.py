"""
Real-Time E-Commerce Intelligence Dashboard
Live dashboard powered by BigQuery + Streamlit
"""
import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# ── Page Config ─────────────────────────────────────
st.set_page_config(
    page_title="Real-Time E-Commerce Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ───────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0D1117; }
    .metric-card {
        background: #161B22;
        border: 1px solid #30363D;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .fraud-alert {
        background: #1A0000;
        border: 1px solid #EF4444;
        border-radius: 8px;
        padding: 10px;
        margin: 5px 0;
    }
    .anomaly-critical {
        background: #1A0000;
        border-left: 4px solid #EF4444;
        padding: 8px;
        margin: 4px 0;
        border-radius: 4px;
    }
    .anomaly-warning {
        background: #1A0E00;
        border-left: 4px solid #F59E0B;
        padding: 8px;
        margin: 4px 0;
        border-radius: 4px;
    }
    .healthy {
        background: #001A0A;
        border-left: 4px solid #22C55E;
        padding: 8px;
        margin: 4px 0;
        border-radius: 4px;
    }
    div[data-testid="metric-container"] {
        background: #161B22;
        border: 1px solid #30363D;
        border-radius: 10px;
        padding: 15px;
    }
</style>
""", unsafe_allow_html=True)

# ── BigQuery Client ──────────────────────────────────
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ecommerce-dashboard-497321")
DATASET    = "realtime_ecommerce"

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def run_query(sql):
    try:
        client = get_bq_client()
        df     = client.query(sql).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# ── Data Functions ───────────────────────────────────
def get_summary_stats():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()
    min5_ago = (now - timedelta(minutes=5)).isoformat()

    df = run_query(f"""
        SELECT
            COUNT(*)                           AS total_orders,
            ROUND(SUM(total_amount), 2)        AS total_revenue,
            ROUND(AVG(total_amount), 2)        AS avg_order,
            COUNTIF(is_fraud = true)           AS fraud_count,
            COUNT(DISTINCT customer_location)  AS cities,
            COUNT(DISTINCT product_name)       AS products,
            COUNTIF(processed_at >= '{min5_ago}')
                                               AS orders_5min,
            ROUND(SUM(
                CASE WHEN processed_at >= '{min5_ago}'
                THEN total_amount ELSE 0 END), 2)
                                               AS revenue_5min
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
            COUNT(*)                               AS orders,
            ROUND(SUM(total_amount), 2)            AS revenue,
            COUNTIF(is_fraud = true)               AS fraud
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY minute
        ORDER BY minute
    """)

def get_top_products():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()

    return run_query(f"""
        SELECT
            product_name,
            COUNT(*)                    AS orders,
            ROUND(SUM(total_amount), 2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY product_name
        ORDER BY revenue DESC
        LIMIT 8
    """)

def get_channel_breakdown():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()

    return run_query(f"""
        SELECT
            sales_channel,
            COUNT(*)                    AS orders,
            ROUND(SUM(total_amount), 2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY sales_channel
        ORDER BY revenue DESC
    """)

def get_city_breakdown():
    now      = datetime.now(timezone.utc)
    hour_ago = (now - timedelta(hours=1)).isoformat()

    return run_query(f"""
        SELECT
            customer_location  AS city,
            COUNT(*)           AS orders,
            ROUND(SUM(total_amount), 2) AS revenue
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        WHERE processed_at >= '{hour_ago}'
        GROUP BY city
        ORDER BY orders DESC
        LIMIT 9
    """)

def get_recent_fraud():
    return run_query(f"""
        SELECT
            fraud_id,
            customer_id,
            fraud_type,
            ROUND(confidence_score * 100, 0) AS confidence_pct,
            ROUND(total_amount, 2)            AS amount,
            flagged_at
        FROM `{PROJECT_ID}.{DATASET}.fraud_detection`
        ORDER BY flagged_at DESC
        LIMIT 10
    """)

def get_recent_anomalies():
    return run_query(f"""
        SELECT
            anomaly_type,
            severity,
            description,
            recommendation,
            detected_at
        FROM `{PROJECT_ID}.{DATASET}.anomaly_events`
        ORDER BY detected_at DESC
        LIMIT 8
    """)

def get_live_orders():
    return run_query(f"""
        SELECT
            order_id,
            customer_id,
            product_name,
            category,
            ROUND(total_amount, 2) AS amount,
            sales_channel,
            customer_location,
            is_fraud,
            processed_at
        FROM `{PROJECT_ID}.{DATASET}.streaming_orders`
        ORDER BY processed_at DESC
        LIMIT 20
    """)

# ── Dashboard Layout ─────────────────────────────────
def main():
    # Header
    st.markdown("""
    <div style='text-align:center; padding: 20px 0 10px 0'>
        <h1 style='color:#F9FAFB; font-size:2.2rem; margin:0'>
            🛒 Real-Time E-Commerce Intelligence
        </h1>
        <p style='color:#6B7280; margin:5px 0'>
            Live pipeline powered by BigQuery + Gemini AI
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.markdown("## ⚙️ Controls")
        refresh = st.slider(
            "Auto-refresh (seconds)", 10, 120, 30
        )
        st.markdown("---")
        st.markdown("### 📊 Data Source")
        st.markdown(f"**Project:** {PROJECT_ID}")
        st.markdown(f"**Dataset:** {DATASET}")
        st.markdown("---")
        st.markdown("### 🔗 Links")
        st.markdown("[GitHub Repo](https://github.com/Jeevan1122/realtime-ecommerce-platform)")
        st.markdown("---")
        last_refresh = datetime.now().strftime("%H:%M:%S")
        st.markdown(f"**Last refresh:** {last_refresh}")
        if st.button("🔄 Refresh Now"):
            st.rerun()

    # ── KPI METRICS ROW ──────────────────────────────
    stats = get_summary_stats()

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric(
            "Total Orders",
            f"{int(stats.get('total_orders', 0)):,}",
            f"+{int(stats.get('orders_5min', 0))} (5min)"
        )
    with col2:
        rev = stats.get('total_revenue', 0)
        r5  = stats.get('revenue_5min', 0)
        st.metric(
            "Revenue",
            f"${rev:,.0f}",
            f"+${r5:,.0f} (5min)"
        )
    with col3:
        avg = stats.get('avg_order', 0)
        st.metric("Avg Order", f"${avg:,.2f}")
    with col4:
        fraud = int(stats.get('fraud_count', 0))
        total = int(stats.get('total_orders', 1))
        rate  = (fraud/total*100) if total > 0 else 0
        st.metric(
            "Fraud Events",
            f"{fraud}",
            f"{rate:.1f}% rate",
            delta_color="inverse"
        )
    with col5:
        st.metric(
            "Cities",
            f"{int(stats.get('cities', 0))}"
        )
    with col6:
        st.metric(
            "Products",
            f"{int(stats.get('products', 0))}"
        )

    st.markdown("---")

    # ── ROW 2: Orders over time + Channel breakdown ──
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown("#### 📈 Orders & Revenue Over Time")
        df_time = get_orders_over_time()
        if not df_time.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_time["minute"],
                y=df_time["orders"],
                name="Orders",
                marker_color="#3B82F6",
                opacity=0.8
            ))
            fig.add_trace(go.Scatter(
                x=df_time["minute"],
                y=df_time["fraud"],
                name="Fraud",
                mode="lines+markers",
                line=dict(color="#EF4444", width=2),
                yaxis="y2"
            ))
            fig.update_layout(
                paper_bgcolor="#161B22",
                plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"),
                yaxis=dict(title="Orders", color="#3B82F6"),
                yaxis2=dict(
                    title="Fraud",
                    overlaying="y",
                    side="right",
                    color="#EF4444"
                ),
                legend=dict(
                    bgcolor="#161B22",
                    bordercolor="#30363D"
                ),
                height=300,
                margin=dict(l=0, r=0, t=10, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for data...")

    with col_right:
        st.markdown("#### 📡 Sales Channels")
        df_chan = get_channel_breakdown()
        if not df_chan.empty:
            fig = px.pie(
                df_chan,
                values="revenue",
                names="sales_channel",
                color_discrete_sequence=[
                    "#3B82F6","#8B5CF6","#14B8A6",
                    "#F59E0B","#EF4444"
                ],
                hole=0.4
            )
            fig.update_layout(
                paper_bgcolor="#161B22",
                font=dict(color="#9CA3AF"),
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=True,
                legend=dict(bgcolor="#161B22")
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for data...")

    # ── ROW 3: Top Products + City ───────────────────
    col_prod, col_city = st.columns(2)

    with col_prod:
        st.markdown("#### 🏆 Top Products by Revenue")
        df_prod = get_top_products()
        if not df_prod.empty:
            fig = px.bar(
                df_prod,
                x="revenue",
                y="product_name",
                orientation="h",
                color="revenue",
                color_continuous_scale="Blues",
                text="orders"
            )
            fig.update_layout(
                paper_bgcolor="#161B22",
                plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"),
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False,
                yaxis=dict(categoryorder="total ascending")
            )
            fig.update_traces(
                texttemplate="%{text} orders",
                textposition="outside"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for data...")

    with col_city:
        st.markdown("#### 🌆 Orders by City")
        df_city = get_city_breakdown()
        if not df_city.empty:
            fig = px.bar(
                df_city,
                x="city",
                y="orders",
                color="revenue",
                color_continuous_scale="Teal",
                text="orders"
            )
            fig.update_layout(
                paper_bgcolor="#161B22",
                plot_bgcolor="#0D1117",
                font=dict(color="#9CA3AF"),
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False
            )
            fig.update_traces(
                texttemplate="%{text}",
                textposition="outside"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for data...")

    st.markdown("---")

    # ── ROW 4: Fraud + Anomalies ─────────────────────
    col_fraud, col_anom = st.columns(2)

    with col_fraud:
        st.markdown("#### 🚨 Recent Fraud Events")
        df_fraud = get_recent_fraud()
        if not df_fraud.empty:
            for _, row in df_fraud.iterrows():
                conf = row.get("confidence_pct", 0)
                col  = "#EF4444" if conf >= 80 \
                       else "#F59E0B"
                st.markdown(f"""
                <div class='fraud-alert'>
                    <span style='color:{col}; font-weight:bold'>
                        ⚠️ {row['customer_id']}
                    </span>
                    <span style='color:#9CA3AF; float:right'>
                        ${row['amount']:,.2f}
                    </span><br>
                    <span style='color:#6B7280; font-size:0.8rem'>
                        {row['fraud_type']} ·
                        {int(conf)}% confidence
                    </span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class='healthy'>
                ✅ No fraud events detected recently
            </div>
            """, unsafe_allow_html=True)

    with col_anom:
        st.markdown("#### 🤖 AI Anomaly Alerts")
        df_anom = get_recent_anomalies()
        if not df_anom.empty:
            for _, row in df_anom.iterrows():
                sev   = row.get("severity", "INFO")
                css   = "anomaly-critical" \
                        if sev == "CRITICAL" \
                        else "anomaly-warning"
                icon  = "🔴" if sev == "CRITICAL" \
                        else "🟡"
                st.markdown(f"""
                <div class='{css}'>
                    <b style='color:#F9FAFB'>
                        {icon} {row['anomaly_type']}
                    </b><br>
                    <span style='color:#9CA3AF; font-size:0.82rem'>
                        {row['description'][:80]}...
                    </span><br>
                    <span style='color:#6B7280; font-size:0.75rem'>
                        → {row['recommendation'][:60]}
                    </span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class='healthy'>
                ✅ No anomalies detected — pipeline healthy!
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── ROW 5: Live Orders Feed ───────────────────────
    st.markdown("#### ⚡ Live Order Feed (Last 20)")
    df_orders = get_live_orders()
    if not df_orders.empty:
        df_display = df_orders.copy()
        df_display["amount"]    = df_display["amount"].apply(
            lambda x: f"${x:,.2f}"
        )
        df_display["is_fraud"]  = df_display["is_fraud"].apply(
            lambda x: "🚨 FRAUD" if x else "✅ OK"
        )
        df_display["processed_at"] = pd.to_datetime(
            df_display["processed_at"]
        ).dt.strftime("%H:%M:%S")

        df_display = df_display[[
            "processed_at", "product_name",
            "amount", "sales_channel",
            "customer_location", "is_fraud"
        ]].rename(columns={
            "processed_at"    : "Time",
            "product_name"    : "Product",
            "amount"          : "Amount",
            "sales_channel"   : "Channel",
            "customer_location": "City",
            "is_fraud"        : "Status"
        })

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("Waiting for live orders...")

    # ── Footer ────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"""
    <div style='text-align:center; color:#4B5563;
                font-size:0.8rem; padding:10px'>
        Real-Time E-Commerce Intelligence Platform ·
        Built by Kodamati Jeevan Sai ·
        Powered by BigQuery + Gemini AI ·
        Auto-refreshes every {refresh}s
    </div>
    """, unsafe_allow_html=True)

    # ── Auto Refresh ──────────────────────────────────
    time.sleep(refresh)
    st.rerun()

if __name__ == "__main__":
    main()
