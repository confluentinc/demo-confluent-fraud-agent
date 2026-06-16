import json
import os
import threading
import time
from collections import deque
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

# Load Terraform-generated connection config.
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

KAFKA_BOOTSTRAP = os.environ["BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]
SR_URL = os.environ["SCHEMA_REGISTRY_URL"]
SR_API_KEY = os.environ["SCHEMA_REGISTRY_API_KEY"]
SR_API_SECRET = os.environ["SCHEMA_REGISTRY_API_SECRET"]

TOPICS = ["transactions", "user_logins", "account_changes", "fraud_alerts"]
MAX_EVENTS = 500
TIMESERIES_BUCKETS = 30
BUCKET_SECONDS = 10

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon=":shield:",
    layout="wide",
)

CUSTOM_CSS = """
<style>
    [data-testid="stMainBlockContainer"] { padding-top: 2.5rem; }
    [data-testid="stHeader"] { background: rgba(14, 17, 23, 0.95); }
    div[data-testid="stMetric"] {
        background: #1a1a2e;
        border: 1px solid #2a2a4a;
        border-left: 4px solid #4fc3f7;
        border-radius: 8px;
        padding: 15px 20px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.4);
    }
    div[data-testid="stMetric"] label {
        color: #8888aa !important;
        font-size: 0.8rem !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 700 !important;
        color: #e8e8f0 !important;
    }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #c0c0e0;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #2a2a4a;
    }
    .severity-critical {
        background: #d32f2f; color: white; padding: 3px 10px;
        border-radius: 4px; font-weight: 600; font-size: 0.8rem;
    }
    .severity-high {
        background: #e65100; color: white; padding: 3px 10px;
        border-radius: 4px; font-weight: 600; font-size: 0.8rem;
    }
    .severity-medium {
        background: #f9a825; color: #1a1a2e; padding: 3px 10px;
        border-radius: 4px; font-weight: 600; font-size: 0.8rem;
    }
    .severity-low {
        background: #2e7d32; color: white; padding: 3px 10px;
        border-radius: 4px; font-weight: 600; font-size: 0.8rem;
    }
    .topic-dot {
        display: inline-block; width: 10px; height: 10px;
        border-radius: 50%; margin-right: 6px;
    }
</style>
"""

TOPIC_COLORS = {
    "transactions": "#4fc3f7",
    "user_logins": "#81c784",
    "account_changes": "#ffb74d",
    "fraud_alerts": "#ef5350",
}


def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "client.id": "fraud-demo-dashboard",
        "group.id": "dashboard-streamlit-cc",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def create_avro_deserializer():
    """Generic Avro deserializer — resolves each message's writer schema from
    Schema Registry by id, so one instance decodes all four topics."""
    sr = SchemaRegistryClient({
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    })
    return AvroDeserializer(sr)


def get_bucket_key():
    now = time.time()
    return int(now // BUCKET_SECONDS) * BUCKET_SECONDS


def _coerce_list(value):
    """fraud_alerts stores actions_taken / flagged_transaction_ids as JSON-array
    strings; turn them back into Python lists for display."""
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else [value]
        except json.JSONDecodeError:
            return [value]
    return []


def process_message(topic, value, batch):
    ts = datetime.now().strftime("%H:%M:%S")
    user_id = value.get("user_id", "N/A")

    if topic == "transactions":
        summary = f"${value.get('amount', 0):.2f} at {value.get('merchant', '?')} — {value.get('location', '?')}"
    elif topic == "user_logins":
        summary = f"{value.get('location', '?')} via {value.get('device_id', '?')}"
    elif topic == "account_changes":
        summary = f"{value.get('field_changed', '?')}: {value.get('old_value', '?')} → {value.get('new_value', '?')}"
    elif topic == "fraud_alerts":
        value["actions_taken"] = _coerce_list(value.get("actions_taken"))
        value["flagged_transaction_ids"] = _coerce_list(value.get("flagged_transaction_ids"))
        summary = f"Risk {value.get('risk_score', '?')}: {str(value.get('reasoning', '?'))[:100]}"
    else:
        summary = str(value)[:80]

    batch.append((topic, ts, user_id, summary, value))


def kafka_polling_thread(state, lock):
    consumer = create_consumer()
    consumer.subscribe(TOPICS)
    deserialize = create_avro_deserializer()
    try:
        while True:
            batch = []
            for _ in range(100):
                msg = consumer.poll(0.1)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        pass
                    continue
                if msg.value() is None:
                    continue
                try:
                    value = deserialize(
                        msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                except Exception:
                    continue
                if value is not None:
                    process_message(msg.topic(), value, batch)

            if not batch:
                time.sleep(0.2)
                continue

            bucket = get_bucket_key()

            with lock:
                for topic, ts, user_id, summary, value in batch:
                    state["counters"][topic] = state["counters"].get(topic, 0) + 1
                    state["users"].add(user_id)

                    state["events"].appendleft({
                        "time": ts,
                        "topic": topic,
                        "user_id": user_id,
                        "summary": summary,
                    })

                    if topic == "fraud_alerts":
                        state["alerts"].appendleft({"time": ts, **value})
                        state["user_alert_counts"][user_id] = (
                            state["user_alert_counts"].get(user_id, 0) + 1
                        )
                        state["risk_history"].appendleft({
                            "time": ts,
                            "score": value.get("risk_score", 0),
                            "user_id": user_id,
                        })

                    ts_buckets = state["timeseries"]
                    if not ts_buckets or ts_buckets[0]["bucket"] != bucket:
                        ts_buckets.appendleft({
                            "bucket": bucket,
                            "transactions": 0,
                            "user_logins": 0,
                            "account_changes": 0,
                            "fraud_alerts": 0,
                        })
                    ts_buckets[0][topic] = ts_buckets[0].get(topic, 0) + 1
    finally:
        consumer.close()


def get_shared_state():
    if "initialized" not in st.session_state:
        st.session_state.state = {
            "events": deque(maxlen=MAX_EVENTS),
            "alerts": deque(maxlen=100),
            "counters": {},
            "users": set(),
            "timeseries": deque(maxlen=TIMESERIES_BUCKETS),
            "risk_history": deque(maxlen=200),
            "user_alert_counts": {},
        }
        st.session_state.lock = threading.Lock()
        t = threading.Thread(
            target=kafka_polling_thread,
            args=(st.session_state.state, st.session_state.lock),
            daemon=True,
        )
        t.start()
        st.session_state.initialized = True

    return st.session_state.state, st.session_state.lock


def severity_label(score):
    if score >= 80:
        return "critical", "CRITICAL"
    elif score >= 60:
        return "high", "HIGH"
    elif score >= 40:
        return "medium", "MEDIUM"
    return "low", "LOW"


def render_metrics(state):
    txn = state["counters"].get("transactions", 0)
    login = state["counters"].get("user_logins", 0)
    changes = state["counters"].get("account_changes", 0)
    alerts = state["counters"].get("fraud_alerts", 0)
    unique = len(state["users"])

    alerts_list = list(state["alerts"])
    risk_scores = [a.get("risk_score", 0) for a in alerts_list]
    high_risk = sum(1 for s in risk_scores if s >= 70)
    avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Transactions", f"{txn:,}")
    c2.metric("Logins", f"{login:,}")
    c3.metric("Acct Changes", f"{changes:,}")
    c4.metric("Fraud Alerts", f"{alerts:,}")
    c5.metric("High Risk", high_risk)
    c6.metric("Unique Users", unique)

    return alerts_list, avg_risk


def render_charts(state):
    st.markdown('<p class="section-header">Activity Monitor</p>', unsafe_allow_html=True)

    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.caption("Events Over Time (by topic)")
        ts_data = list(state["timeseries"])
        if ts_data:
            ts_data.reverse()
            rows = []
            for b in ts_data:
                t = datetime.fromtimestamp(b["bucket"]).strftime("%H:%M:%S")
                for topic in TOPICS:
                    rows.append({
                        "Time": t,
                        "Topic": topic,
                        "Count": b.get(topic, 0),
                    })
            df = pd.DataFrame(rows)
            chart = (
                alt.Chart(df)
                .mark_area(opacity=0.7, interpolate="monotone")
                .encode(
                    x=alt.X("Time:N", title=None, axis=alt.Axis(labelAngle=-45, labelColor="#8888aa", gridColor="#2a2a4a")),
                    y=alt.Y("Count:Q", stack=True, title="Events", axis=alt.Axis(labelColor="#8888aa", gridColor="#2a2a4a")),
                    color=alt.Color(
                        "Topic:N",
                        scale=alt.Scale(
                            domain=TOPICS,
                            range=[TOPIC_COLORS[t] for t in TOPICS],
                        ),
                        legend=alt.Legend(orient="bottom", title=None, labelColor="#c0c0e0"),
                    ),
                    tooltip=["Time", "Topic", "Count"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart.configure_view(stroke=None), use_container_width=True)
        else:
            st.info("Waiting for events...")

    with chart_right:
        st.caption("Fraud Alert Risk Scores")
        risk_data = list(state["risk_history"])
        if risk_data:
            risk_data.reverse()
            df = pd.DataFrame(risk_data)
            df["severity"] = df["score"].apply(
                lambda s: "Critical" if s >= 80 else "High" if s >= 60 else "Medium" if s >= 40 else "Low"
            )
            points = (
                alt.Chart(df)
                .mark_circle(size=120, opacity=0.85)
                .encode(
                    x=alt.X("time:N", title=None, axis=alt.Axis(labelAngle=-45, labelColor="#8888aa", gridColor="#2a2a4a")),
                    y=alt.Y("score:Q", title="Risk Score", scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(labelColor="#8888aa", gridColor="#2a2a4a")),
                    color=alt.Color(
                        "severity:N",
                        scale=alt.Scale(
                            domain=["Critical", "High", "Medium", "Low"],
                            range=["#ef5350", "#ff8c00", "#ffd700", "#44bb44"],
                        ),
                        legend=alt.Legend(orient="bottom", title=None, labelColor="#c0c0e0"),
                    ),
                    tooltip=["time", "user_id", "score", "severity"],
                )
                .properties(height=300)
            )
            rule = (
                alt.Chart(pd.DataFrame({"y": [70]}))
                .mark_rule(color="#ef5350", strokeDash=[4, 4], opacity=0.5)
                .encode(y="y:Q")
            )
            st.altair_chart((points + rule).configure_view(stroke=None), use_container_width=True)
        else:
            st.info("Waiting for fraud alerts...")

    user_counts = state["user_alert_counts"]
    if user_counts:
        st.caption("Alerts by User")
        df = pd.DataFrame(
            [{"User": u, "Alerts": c} for u, c in sorted(user_counts.items(), key=lambda x: -x[1])]
        )
        chart = (
            alt.Chart(df)
            .mark_bar(cornerRadiusEnd=4, opacity=0.85)
            .encode(
                x=alt.X("Alerts:Q", title="Alert Count", axis=alt.Axis(labelColor="#8888aa", gridColor="#2a2a4a")),
                y=alt.Y("User:N", sort="-x", title=None, axis=alt.Axis(labelColor="#c0c0e0")),
                color=alt.value("#ef5350"),
                tooltip=["User", "Alerts"],
            )
            .properties(height=max(len(df) * 45, 120))
        )
        st.altair_chart(chart.configure_view(stroke=None), use_container_width=True)


def render_alerts_table(alerts_list):
    st.markdown('<p class="section-header">Recent Fraud Alerts</p>', unsafe_allow_html=True)
    if not alerts_list:
        st.info("No fraud alerts yet. Waiting for the Flink agent to produce alerts...")
        return

    widths = [1, 1.3, 0.8, 1, 4, 2]
    header = st.columns(widths)
    for col, title in zip(header, ["Severity", "User", "Score", "Time", "Reasoning", "Actions"]):
        col.markdown(f"**{title}**")

    for a in alerts_list[:15]:
        score = a.get("risk_score", 0)
        cls, label = severity_label(score)
        user = a.get("user_id", "?")
        reasoning = a.get("reasoning", "")[:150]
        actions = ", ".join(a.get("actions_taken", [])) or "—"
        alert_time = a.get("time", "")

        cols = st.columns(widths)
        with cols[0]:
            st.markdown(f'<span class="severity-{cls}">{label}</span>', unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f"**{user}**")
        with cols[2]:
            st.markdown(f"Score: **{score}**")
        with cols[3]:
            st.markdown(f"`{alert_time}`")
        with cols[4]:
            st.markdown(f"{reasoning}")
        with cols[5]:
            st.markdown(f"`{actions}`")


def render_event_feed(events_snapshot):
    st.markdown('<p class="section-header">Live Event Feed</p>', unsafe_allow_html=True)
    if not events_snapshot:
        st.info("No events yet. Make sure the producer is running...")
        return

    header = st.columns([1, 1.5, 2, 6])
    header[0].markdown("**Time**")
    header[1].markdown("**Topic**")
    header[2].markdown("**User**")
    header[3].markdown("**Details**")

    for e in events_snapshot[:50]:
        topic = e["topic"]
        color = TOPIC_COLORS.get(topic, "#999")
        cols = st.columns([1, 1.5, 2, 6])
        with cols[0]:
            st.text(e["time"])
        with cols[1]:
            st.markdown(
                f'<span class="topic-dot" style="background:{color}"></span>{topic}',
                unsafe_allow_html=True,
            )
        with cols[2]:
            st.text(e["user_id"])
        with cols[3]:
            st.text(e["summary"])


def main():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    state, lock = get_shared_state()

    st.markdown("## :shield: Fraud Detection Dashboard")
    st.caption(f"Real-time monitoring · Kafka @ `{KAFKA_BOOTSTRAP}`")

    with lock:
        snapshot = {
            "counters": dict(state["counters"]),
            "users": set(state["users"]),
            "alerts": list(state["alerts"]),
            "events": list(state["events"]),
            "timeseries": list(state["timeseries"]),
            "risk_history": list(state["risk_history"]),
            "user_alert_counts": dict(state["user_alert_counts"]),
        }

    alerts_list, avg_risk = render_metrics(snapshot)

    st.markdown("")
    render_charts(snapshot)

    st.markdown("")
    render_alerts_table(alerts_list)

    st.markdown("")
    render_event_feed(snapshot["events"])

    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()
