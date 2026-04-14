import pandas as pd

def save_to_csv(df: pd.DataFrame, output_path: str):
    """
    Saves transformed data to CSV.
    """
    df.to_csv(output_path, index=False)
    