# %%

import pandas as pd

sales_invoice_filepath = "out/datasets/sales_invoice_line.csv"
output_filepath = "out/reports/revenue.csv"

# max number of difference in days to still consider recurring
recurring_days_thresold = 5

# max number of difference in days to still consider reoccurring
reoccurring_days_thresold = 0

# max number of consecutive years to consider reoccurring
reoccurring_max_period_count = 3  # 3 years in our case

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


def get_next_smallest(values: list, value: any, count: int) -> list:
    """
    returns a list or next elements with max size of count.
    """
    sorted_values = sorted(values)
    next_values = [y for y in sorted_values if y > value][:count]
    return next_values


def get_category(records: pd.DataFrame, year_groups) -> str:
    intervals = records["invoice_date"].sort_values().diff().dt.days[1:]

    if intervals.max() - intervals.min() <= recurring_days_thresold:
        return "recurring"

    year = records.iloc[0]["year"]
    years = year_groups.keys()
    next_years = get_next_smallest(years, year, reoccurring_max_period_count)
    next_year_count = len(next_years)

    if next_year_count > 0:
        next_years_invoice_counts = [len(year_groups[year]) for year in next_years]
        next_years_min_invoice_count = min(next_years_invoice_counts)
        next_years_max_invoice_count = max(next_years_invoice_counts)
        next_years_max_invoice_count_diff = (
            next_years_max_invoice_count - next_years_min_invoice_count
        )

        if next_years_max_invoice_count_diff <= reoccurring_days_thresold:
            return "reoccuring"

    return "non-reoccurring"


def get_categorizations(records: pd.DataFrame) -> pd.DataFrame:

    categorizations = list()

    year_groups = records.groupby("year").groups
    for year, index in year_groups.items():
        year_records = df.loc[index]
        year_record_count = len(year_records)
        year_total_amount = int(year_records["amount"].sum())
        category = get_category(year_records, year_groups=year_groups)

        categorizations.append(
            {
                "customer_id": customer_id,
                "year": year,
                "invoice_count": year_record_count,
                "total_amount": year_total_amount,
                "category": category,
            }
        )

    categorizations_df = pd.DataFrame(categorizations)
    return categorizations_df


df["year"] = df["invoice_date"].dt.year
customer_groups = df.groupby("customer_id").groups

categorizations = list[pd.DataFrame]()
for customer_id, index in customer_groups.items():
    customer_records = df.loc[index]

    customer_categorizations = get_categorizations(customer_records)
    categorizations.append(customer_categorizations)

categorizations_df = pd.concat(categorizations)

# %%

categorizations_df.to_csv(output_filepath, index=False)
# %%
