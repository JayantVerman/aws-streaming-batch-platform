"""Zero-install dashboard over the serving schema (guide: Streamlit fallback
for visitors without Power BI). Reads Postgres directly via SQLAlchemy.

Run:  bash viz/streamlit_app/run_streamlit.sh   (or `python -m streamlit run ...`)
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

DB_URL = os.getenv("DB_URL", "postgresql://platform:platform@localhost:5432/warehouse")

engine = create_engine(DB_URL)

st.set_page_config(page_title="Retail Platform — Serving", layout="wide")
st.title("Retail Serving Layer")

QUERIES = {
    "Daily sales": (
        "SELECT order_date, total_orders, total_items, total_revenue "
        "FROM serving.daily_sales ORDER BY order_date"),
    "Category sales": (
        "SELECT category_name, total_items, total_revenue "
        "FROM serving.category_sales ORDER BY total_revenue DESC"),
    "Customer orders": (
        "SELECT customer_name, first_order, last_order, total_orders "
        "FROM serving.customer_orders ORDER BY total_orders DESC LIMIT 500"),
    "Busiest days": (
        "SELECT order_date, total_orders, total_revenue "
        "FROM serving.daily_sales ORDER BY total_revenue DESC LIMIT 10"),
}

tab = st.sidebar.radio("Report", list(QUERIES))
with engine.connect() as conn:
    df = pd.read_sql(QUERIES[tab], conn)

st.subheader(tab)
st.dataframe(df, use_container_width=True)
if "order_date" in df.columns:
    st.line_chart(df.set_index("order_date")["total_revenue"] if "total_revenue" in df else df.set_index("order_date"))