# %%

import pandas as pd

sales_invoice_filepath = "out/datasets/sales_invoice_line.csv"
output_filepath = "out/reports/revenue.csv"

# ----------------------------------------------------------------------------
# reading
# ----------------------------------------------------------------------------

df = pd.read_csv(sales_invoice_filepath)
df = df[["Sell_to_Customer_No", "Shipment_Date", "Amount_Including_VAT"]]

df = df.rename(
    columns={
        "Sell_to_Customer_No": "customer_id",
        "Shipment_Date": "invoice_date",
        "Amount_Including_VAT": "amount",
    }
)

previous_count = len(df)
df = df.dropna()

dropped_count = len(df) - previous_count
if dropped_count > 0:
    print(f"info: dropped {dropped_count} null values...")

# ----------------------------------------------------------------------------
# cleaning
# ----------------------------------------------------------------------------
df["customer_id"] = (
    pd.to_numeric(df["customer_id"], errors="coerce").dropna().astype(int)
)
# df["customer_id"] = df["customer_id"].astype(int)
df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
df["amount"] = df["amount"].astype(int)

previous_count = len(df)
df = df.dropna()

dropped_count = len(df) - previous_count
if dropped_count > 0:
    print(f"info: dropped {dropped_count} null values after converting types...")

# ----------------------------------------------------------------------------
# processing
# ----------------------------------------------------------------------------

# %%


def get_category(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "single"

    intervals = df["invoice_date"].sort_values().diff().dt.days[1:]

    if intervals.max() - intervals.min() <= 5:
        return "recurring"
    else:
        return "reoccurring"


groups = df.groupby("customer_id").groups

output = list()
for customer_id, index in groups.items():
    category = get_category(df.loc[index])
    output.append({"customer_id": customer_id, "category": category})

# %%

output_df = pd.DataFrame(output)
output_df = output_df.set_index("customer_id")
output_df.to_csv(output_filepath)
