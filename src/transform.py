import pandas as pd

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans sales data by handling nulls and formatting dates.
    """
    df = df.dropna()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["amount"] = df["amount"].astype(float)
    return df


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds derived columns like month and revenue category.
    """
    df["month"] = df["order_date"].dt.to_period("M")

    df["revenue_bucket"] = pd.cut(
        df["amount"],
        bins=[0, 50, 150, 500],
        labels=["Low", "Medium", "High"]
    )
    return df


def aggregate_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates sales by region and month.
    """
    agg_df = (
        df.groupby(["region", "month"])
        .agg(
            total_revenue=("amount", "sum"),
            total_orders=("order_id", "count"),
            unique_customers=("customer_id", "nunique")
        )
        .reset_index()
    )

    return agg_df
