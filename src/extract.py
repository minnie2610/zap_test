import pandas as pd

def read_sales_data(file_path: str) -> pd.DataFrame:
    """
    Reads raw sales data from CSV file.

    Args:
        file_path (str): Path to CSV file

    Returns:
        pd.DataFrame: Raw sales dataframe
    """
    df = pd.read_csv(file_path)
    return df