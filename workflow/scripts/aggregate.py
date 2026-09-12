"""Aggregate cleaned transactions into per-category sales summaries."""

import sys

import pandas as pd

source, destination, category = sys.argv[1], sys.argv[2], sys.argv[3]
df = pd.read_csv(source)

df = df[df["product"] == category]

summary = (
    df.groupby("store_id")
    .agg(
        transaction_count=("id", "count"),
        total_quantity=("quantity", "sum"),
        avg_quantity=("quantity", "mean"),
        total_revenue=("revenue", "sum"),
        avg_revenue=("revenue", "mean"),
        min_revenue=("revenue", "min"),
        max_revenue=("revenue", "max"),
        first_date=("date", "min"),
        last_date=("date", "max"),
    )
    .reset_index()
)
summary.insert(1, "product", category)

summary.to_csv(destination, index=False)
