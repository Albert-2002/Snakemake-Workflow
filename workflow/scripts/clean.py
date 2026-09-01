"""Clean raw transaction data."""

import sys

import numpy as np
import pandas as pd

source, destination = sys.argv[1], sys.argv[2]
df = pd.read_csv(source)

# Standardize column names
df.columns = df.columns.str.strip().str.lower()
df = df.rename(
    columns={
        "filiaalnr": "store_id",
        "product_category": "product",
        "amount": "revenue",
        "transaction_date": "date",
        "transaction_id": "id",
    }
)

# Remove rows with missing values in critical columns
df = df.dropna(subset=["store_id", "date", "product", "quantity", "revenue"])

# Remove rows with invalid numeric values (inf, -inf)
df = df[~df["quantity"].isin([np.inf, -np.inf])]
df = df[~df["revenue"].isin([np.inf, -np.inf])]

# Keep only rows with non-negative quantities and revenue
df = df[(df["quantity"] >= 0) & (df["revenue"] >= 0)]

# Select and reorder relevant columns
df = df[["id", "store_id", "product", "quantity", "revenue", "date"]]

df.to_csv(destination, index=False)
