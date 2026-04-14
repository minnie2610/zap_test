import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load raw data from CSV.
    """
    logging.info(f"Loading data from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} records")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate required columns and remove invalid records.
    """
    required_cols = ["order_id", "customer_id", "amount", "order_date"]

    logging.info("Validating data...")

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Remove negative or zero amounts
    df = df[df["amount"] > 0]

    # Convert date
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Drop invalid dates
    df = df.dropna(subset=["order_date"])

    logging.info(f"Data after validation: {len(df)} records")
    return df


def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records based on order_id.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    after = len(df)

    logging.info(f"Removed {before - after} duplicate records")
    return df


def calculate_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate business KPIs.
    """
    logging.info("Calculating KPIs...")

    df["month"] = df["order_date"].dt.to_period("M")

    kpi_df = df.groupby("month").agg(
        total_revenue=("amount", "sum"),
        avg_order_value=("amount", "mean"),
        total_orders=("order_id", "count"),
        unique_customers=("customer_id", "nunique")
    ).reset_index()

    logging.info("KPI calculation completed")
    return kpi_df


def save_output(df: pd.DataFrame, output_path: str):
    """
    Save processed data to CSV.
    """
    df.to_csv(output_path, index=False)
    logging.info(f"Output saved to {output_path}")


def run_pipeline():
    """
    End-to-end pipeline execution.
    """
    input_path = "data/sales_data.csv"
    output_path = f"data/kpi_output_{datetime.now().strftime('%Y%m%d')}.csv"

    logging.info("Pipeline started")

    df = load_data(input_path)
    df = validate_data(df)
    df = deduplicate_data(df)
    kpi_df = calculate_kpis(df)

    save_output(kpi_df, output_path)

    logging.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()