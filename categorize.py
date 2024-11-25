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
df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

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
    """
        If
    """
    if len(df) < 2:
        return "non-reoccurring"

    intervals = df["invoice_date"].sort_values().diff().dt.days[1:]

    if intervals.max() - intervals.min() <= 5:
        return "recurring"
    else:
        return "re-occurring"


def get_month_name(month: int) -> str | None:
    return {
        1: "jan",
        2: "feb",
        3: "mar",
        4: "apr",
        5: "may",
        6: "jun",
        7: "jul",
        8: "aug",
        9: "sep",
        10: "oct",
        11: "nov",
        12: "dec",
    }.get(month)


df["year"] = df["invoice_date"].dt.to_period("Y")
groups = df.groupby(["customer_id", "year"]).groups

output = list()
for key, index in groups.items():
    customer_id, year_month_period = key
    records = df.loc[index]
    record_count = len(records)
    year = year_month_period.year
    total_amount = int(records["amount"].sum())
    category = get_category(records)

    output.append(
        {
            "customer_id": customer_id,
            "year": year,
            "invoice_count": record_count,
            "total_amount": total_amount,
            "category": category,
        }
    )

# %%

output_df = pd.DataFrame(output)
output_df.to_csv(output_filepath, index=False)

# %%
