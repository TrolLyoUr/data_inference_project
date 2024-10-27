import pandas as pd
import numpy as np
import os
from dateutil.parser import parse
import re

def load_data(file_path, has_headers=True, chunksize=None):
    """
    Load data from a CSV or Excel file into a Pandas DataFrame.
    
    Parameters:
    - file_path (str): The path to the CSV or Excel file.
    - has_headers (bool): Whether the file has headers in the first row.
    - chunksize (int, optional): The number of rows per chunk (for large files).
    
    Returns:
    - df (DataFrame or TextFileReader): The loaded DataFrame or an iterator for chunked processing.
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.csv':
        if has_headers:
            return pd.read_csv(file_path, chunksize=chunksize, low_memory=False)
        else:
            # Generate column names if no headers
            df = pd.read_csv(file_path, header=None, chunksize=chunksize, low_memory=False)
            if not chunksize:
                df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
            return df
    elif file_ext in ['.xls', '.xlsx']:
        if has_headers:
            return pd.read_excel(file_path, chunksize=chunksize)
        else:
            df = pd.read_excel(file_path, header=None, chunksize=chunksize)
            if not chunksize:
                df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
            return df
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")

def infer_and_convert_dtypes(df, type_overrides=None):
    """
    Infer and convert data types for each column in the DataFrame in-place.
    
    Parameters:
    - df (DataFrame): The DataFrame to process.
    - type_overrides (dict, optional): A dictionary mapping column names to desired data types.
    Returns:
    - inferred_types (dict): A dictionary mapping column names to inferred data types.
    - conversion_errors (dict): A dictionary mapping column names to conversion error messages.
    """
    if type_overrides is None:
        type_overrides = {}
    inferred_types = {}
    conversion_errors = {}
    
    # Define missing values and patterns
    missing_values = ["NA", "NaN", "N/A", "Not Available", "null", "", " ", "--", "-", "?", "missing", "undefined", "unknown", "N.A."]
    missing_value_patterns = [r'^\s*$', r'^-*$', r'^null$', r'^n/?a$', r'^not available$', r'^missing$', r'^undefined$', r'^unknown$', r'^n\.a\.$']
    missing_value_regex = re.compile('|'.join(missing_value_patterns), flags=re.IGNORECASE)
    
    for col in df.columns:
        col_series = df[col]
        # Apply type override if specified
        if col in type_overrides:
            try:
                df[col] = col_series.astype(type_overrides[col], errors='raise')
                inferred_types[col] = type_overrides[col]
                continue
            except (ValueError, TypeError) as e:
                # Store the error message for this column
                error_msg = str(e)
                conversion_errors[col] = {
                    'requested_type': type_overrides[col],
                    'error': error_msg,
                    'sample_values': col_series.dropna().head(5).tolist()
                }
                # Continue with normal inference for this column
        
        # Replace missing value strings with np.nan
        col_series.replace(missing_values, np.nan, inplace=True)
        col_series.replace(missing_value_regex, np.nan, inplace=True)
        # Skip columns with all NaN values
        if col_series.dropna().empty:
            inferred_types[col] = 'empty'
            continue

        # Try to infer as datetime
        col_datetime = pd.to_datetime(col_series, errors='coerce', format='mixed')
        if col_datetime.notna().sum() / col_series.notna().sum() > 0.9:
            df[col] = col_datetime
            inferred_types[col] = 'datetime64[ns]'
            continue
        else:
            # Attempt parsing with dateutil
            def try_parse_date(x):
                if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
                    # Skip numeric-only entries (likely not dates)
                    return np.nan
                try:
                    return parse(str(x))
                except (ValueError, TypeError):
                    return np.nan
            col_datetime = col_series.apply(try_parse_date)
            if col_datetime.notna().sum() / col_series.notna().sum() > 0.9:
                df[col] = col_datetime
                inferred_types[col] = 'datetime64[ns]'
                continue

        # Check for boolean
        boolean_values = {'0', '1', 0, 1, True, False, 'True', 'False', 'true', 'false',
                          'yes', 'no', 'y', 'n', 't', 'f', 'on', 'off', 'enabled', 'disabled'}
        true_values = {'1', 1, True, 'True', 'true', 'yes', 'y', 't', 'on', 'enabled'}
        false_values = {'0', 0, False, 'False', 'false', 'no', 'n', 'f', 'off', 'disabled'}
        unique_values = set(str(x).strip().lower() for x in col_series.dropna().unique())
        if unique_values.issubset(boolean_values):
            df[col] = col_series.apply(lambda x: True if str(x).strip().lower() in true_values else False if str(x).strip().lower() in false_values else np.nan)
            inferred_types[col] = 'bool'
            continue

        # Attempt to clean and convert to numeric
        def clean_numeric(x):
            if pd.isnull(x):
                return np.nan
            x_str = str(x).strip()
            x_str = re.sub(r'[^\d.,\-+eE]', '', x_str)
            if x_str.count(',') > x_str.count('.'):
                x_str = x_str.replace('.', '').replace(',', '.')
            else:
                x_str = x_str.replace(',', '')
            try:
                return float(x_str)
            except ValueError:
                return np.nan

        col_numeric = col_series.apply(clean_numeric)
        if col_numeric.notna().sum() / col_series.notna().sum() > 0.9:
            df[col] = col_numeric
            # Downcast numeric types
            df[col] = pd.to_numeric(df[col], downcast='float')
            inferred_types[col] = str(df[col].dtype)
            continue

        # Check for categorical
        if len(col_series) < 1000:
            max_unique_ratio = 0.5  # More lenient for small datasets
        elif len(col_series) < 10000:
            max_unique_ratio = 0.2  # Stricter for medium datasets
        else:
            max_unique_ratio = 0.1  # Most strict for large datasets

        unique_ratio = col_series.nunique(dropna=True) / len(col_series)
        if unique_ratio <= max_unique_ratio:
            df[col] = col_series.astype('category')
            inferred_types[col] = 'category'
            continue

        # Default to string
        df[col] = col_series.astype('string')
        # Normalize strings
        df[col] = df[col].str.strip().str.lower()
        inferred_types[col] = 'string'

    return inferred_types, conversion_errors

def process_file(file_path, type_overrides=None, has_headers=True):
    """
    Process the file by loading it and inferring data types.
    """
    file_size = os.path.getsize(file_path)
    use_chunking = file_size > 100 * 1024 * 1024  # 100 MB threshold
    
    if use_chunking:
        chunks = load_data(file_path, has_headers=has_headers, chunksize=100000)
        inferred_types = {}
        conversion_errors = {}
        df_list = []
        
        # For the first chunk, generate column names if no headers
        first_chunk = True
        for chunk in chunks:
            if first_chunk and not has_headers:
                chunk.columns = [f'Column_{i+1}' for i in range(len(chunk.columns))]
                first_chunk = False
                
            chunk_inferred_types, chunk_errors = infer_and_convert_dtypes(chunk, type_overrides)
            df_list.append(chunk)
            inferred_types.update(chunk_inferred_types)
            conversion_errors.update(chunk_errors)
            
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = load_data(file_path, has_headers=has_headers)
        inferred_types, conversion_errors = infer_and_convert_dtypes(df, type_overrides)
    
    return df, inferred_types, conversion_errors

# Example usage:
if __name__ == '__main__':
    file_path = 'sample_data.csv'  # Replace with your file path
    df, inferred_types, conversion_errors = process_file(file_path)
    print(df)
    print("Inferred Data Types:")
    for col, dtype in inferred_types.items():
        print(f"{col}: {dtype}")
    print("Conversion Errors:")
    for col, error in conversion_errors.items():
        print(f"{col}: {error['error']}")
