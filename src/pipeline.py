from src.extract import read_sales_data
from src.transform import clean_sales_data, add_derived_metrics, aggregate_sales
from src.load import save_to_csv


def run_pipeline():
    """
    Runs the full ETL pipeline:
    Extract → Transform → Load
    """
    input_path = "data/sales_data.csv"
    output_path = "data/output_sales_summary.csv"

    # Extract
    df = read_sales_data(input_path)

    # Transform
    df = clean_sales_data(df)
    df = add_derived_metrics(df)
    agg_df = aggregate_sales(df)

    # Load
    save_to_csv(agg_df, output_path)

    print("Pipeline executed successfully!")


if __name__ == "__main__":
    run_pipeline()