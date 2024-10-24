import pandas as pd
import numpy as np
import datetime


def load_data(file_path):
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def infer_data_types(df):
    inferred_types = {}
    for column in df.columns:
        col_data = df[column]
        # Check for missing values
        if col_data.isnull().all():
            inferred_types[column] = "Unknown (All values are null)"
            continue
        # Try numeric
        try:
            pd.to_numeric(col_data.dropna())
            inferred_types[column] = "Numeric"
            df[column] = pd.to_numeric(col_data, errors="coerce")
            continue
        except:
            pass
        # Try datetime
        try:
            pd.to_datetime(col_data.dropna(), format="mixed")
            inferred_types[column] = "DateTime"
            df[column] = pd.to_datetime(col_data, errors="coerce", format="mixed")
            continue
        except:
            pass
        # Check for boolean
        if col_data.dropna().isin([0, 1, "0", "1", True, False, "True", "False"]).all():
            inferred_types[column] = "Boolean"
            df[column] = col_data.astype("bool")
            continue
        # Check for categorical
        if col_data.nunique() / col_data.count() < 0.05:
            inferred_types[column] = "Categorical"
            df[column] = col_data.astype("category")
            continue
        # Default to object (text)
        inferred_types[column] = "Text"
        df[column] = col_data.astype("object")
    return df, inferred_types


def optimize_dtypes(df):
    for col in df.select_dtypes(include=["int", "float"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    return df


def process_large_csv(file_path, chunk_size=10000):
    inferred_types = {}
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk, chunk_types = infer_data_types(chunk)
        inferred_types.update(chunk_types)
        # You can process or save each chunk here
    return inferred_types
