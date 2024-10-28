import pandas as pd
import numpy as np
import os
from dateutil.parser import parse
import re
import chardet
from typing import Optional, Tuple, Dict
import codecs

def detect_encoding(file_path: str, sample_size: int = 10000) -> str:
    """
    Detect the encoding of a file using chardet.
    
    Parameters:
    - file_path: Path to the file
    - sample_size: Number of bytes to sample for detection
    
    Returns:
    - detected encoding
    """
    with open(file_path, 'rb') as file:
        raw_data = file.read(sample_size)
    result = chardet.detect(raw_data)
    return result['encoding'] or 'utf-8'

def validate_encoding(file_path: str, encoding: str) -> bool:
    """
    Validate if an encoding can read the entire file without errors.
    
    Parameters:
    - file_path: Path to the file
    - encoding: Encoding to validate
    
    Returns:
    - True if encoding is valid, False otherwise
    """
    try:
        with codecs.open(file_path, 'r', encoding=encoding) as f:
            # Read file in chunks to avoid memory issues
            while f.read(1024):
                pass
        return True
    except UnicodeDecodeError:
        return False

def get_valid_encoding(file_path: str) -> str:
    """
    Get a valid encoding for the file, trying multiple options if needed.
    
    Parameters:
    - file_path: Path to the file
    
    Returns:
    - valid encoding
    """
    # Common encodings to try in order of likelihood
    encodings_to_try = [
        detect_encoding(file_path),  # Try detected encoding first
        'utf-8',
        'utf-8-sig',  # UTF-8 with BOM
        'cp1252',     # Windows-1252
        'iso-8859-1', # Latin-1
        'ascii',
        'utf-16',
        'utf-32',
        'big5',       # Traditional Chinese
        'gb2312',     # Simplified Chinese
        'shift-jis',  # Japanese
        'euc-kr',     # Korean
    ]
    
    # Try each encoding
    for encoding in encodings_to_try:
        if encoding and validate_encoding(file_path, encoding):
            return encoding
            
    raise ValueError("Unable to determine valid encoding for the file")

def load_data(file_path: str, has_headers: bool = True, chunksize: Optional[int] = None) -> pd.DataFrame:
    """
    Load data from a CSV or Excel file into a Pandas DataFrame with encoding detection.
    
    Parameters:
    - file_path: Path to the file
    - has_headers: Whether the file has headers
    - chunksize: Number of rows to read at a time
    
    Returns:
    - DataFrame or TextFileReader
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.csv':
        try:
            # First try to detect and validate encoding
            encoding = get_valid_encoding(file_path)
            
            # Common CSV reading parameters
            csv_params = {
                'encoding': encoding,
                'chunksize': chunksize,
                'low_memory': False,
                'on_bad_lines': 'warn',  # Log warning for bad lines instead of failing
                'encoding_errors': 'replace',  # Replace invalid characters
            }
            
            if has_headers:
                df = pd.read_csv(file_path, **csv_params)
            else:
                df = pd.read_csv(file_path, header=None, **csv_params)
                if not chunksize:
                    df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
            
            return df
            
        except Exception as e:
            # If initial reading fails, try with different parameters
            try:
                # Try with more permissive parameters
                csv_params.update({
                    'sep': None,  # Let pandas detect the separator
                    'engine': 'python',  # Python engine is more flexible
                    'encoding_errors': 'replace',
                    'quoting': 3,  # QUOTE_NONE
                    'dtype': str,  # Read everything as string initially
                })
                
                if has_headers:
                    df = pd.read_csv(file_path, **csv_params)
                else:
                    df = pd.read_csv(file_path, header=None, **csv_params)
                    if not chunksize:
                        df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
                
                return df
                
            except Exception as e2:
                raise ValueError(f"Failed to read CSV file. Errors:\n1. {str(e)}\n2. {str(e2)}")
    
    elif file_ext in ['.xls', '.xlsx']:
        try:
            # Excel files don't need encoding detection
            if has_headers:
                return pd.read_excel(file_path, chunksize=chunksize)
            else:
                df = pd.read_excel(file_path, header=None, chunksize=chunksize)
                if not chunksize:
                    df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
                return df
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {str(e)}")
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
    
    # Enhanced missing values detection
    missing_values = [
        "NA", "NaN", "N/A", "Not Available", "null", "", " ", "--", "-", "?",
        "missing", "undefined", "unknown", "N.A.", "none", "nil", "(empty)",
        "not specified", "n/s", "pending", "tbd", "to be determined"
    ]
    missing_value_patterns = [
        r'^\s*$',
        r'^-*$',
        r'^null$',
        r'^n/?a$',
        r'^not\s+available$',
        r'^missing$',
        r'^undefined$',
        r'^unknown$',
        r'^n\.a\.$',
        r'^none$',
        r'^nil$',
        r'^\(empty\)$',
        r'^not\s+specified$',
        r'^n/s$',
        r'^pending$',
        r'^tbd$',
        r'^to\s+be\s+determined$'
    ]
    missing_value_regex = re.compile('|'.join(missing_value_patterns), flags=re.IGNORECASE)
    
    for col in df.columns:
        col_series = df[col]
        
        # Handle type overrides
        if col in type_overrides:
            try:
                if type_overrides[col] in ('Int64', 'float64'):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                else:
                    df[col] = col_series.astype(type_overrides[col], errors='raise')
                inferred_types[col] = type_overrides[col]
                continue
            except (ValueError, TypeError) as e:
                conversion_errors[col] = {
                    'requested_type': type_overrides[col],
                    'error': str(e),
                    'sample_values': col_series.dropna().head(5).tolist()
                }

        # Clean and standardize the data
        if col_series.dtype == 'object':
            col_series = col_series.astype(str).str.strip().str.lower()
            
        # Replace missing values
        col_series.replace(missing_values, np.nan, inplace=True)
        col_series.replace(missing_value_regex, np.nan, inplace=True)

        # Skip empty columns
        if col_series.dropna().empty:
            inferred_types[col] = 'object'
            continue

        # Sample data for faster inference
        sample_size = min(1000, len(col_series))
        col_sample = col_series.sample(n=sample_size) if len(col_series) > sample_size else col_series

        # Try to infer as datetime using multiple approaches
        is_datetime = False
        
        # First try pandas datetime inference
        datetime_series = pd.to_datetime(col_sample, errors='coerce', format='mixed')
        if datetime_series.notna().sum() / col_sample.notna().sum() > 0.9:
            df[col] = pd.to_datetime(col_series, errors='coerce', format='mixed')
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
            col_datetime = col_sample.apply(try_parse_date)
            if col_datetime.notna().sum() / col_sample.notna().sum() > 0.9:
                df[col] = col_datetime
                inferred_types[col] = 'datetime64[ns]'
                continue

        # Enhanced boolean detection
        boolean_values = {
            'true': True, 'false': False,
            'yes': True, 'no': False,
            'y': True, 'n': False,
            't': True, 'f': False,
            '1': True, '0': False,
            'on': True, 'off': False,
            'enabled': True, 'disabled': False,
            'active': True, 'inactive': False,
            'positive': True, 'negative': False,
            'success': True, 'failure': False,
            'pass': True, 'fail': False,
        }
        
        unique_values = set(str(x).strip().lower() for x in col_sample.dropna().unique())
        if unique_values.issubset(boolean_values.keys()):
            df[col] = col_series.map(lambda x: boolean_values.get(str(x).strip().lower(), np.nan))
            inferred_types[col] = 'bool'
            continue
        # Attempt to clean and convert to numeric
        def clean_numeric(x):
            if pd.isnull(x):
                return np.nan
                
            # Convert to string and clean whitespace
            x_str = str(x).strip().lower()
            
            # Handle currency symbols
            currency_symbols = r'[$€£¥]'
            x_str = re.sub(currency_symbols, '', x_str)
            
            # Handle percentages
            if x_str.endswith('%'):
                try:
                    return float(x_str.rstrip('%')) / 100
                except ValueError:
                    return np.nan
            
            # Handle scientific notation
            if 'e' in x_str and not any(c.isalpha() for c in x_str.replace('e', '')):
                try:
                    return float(x_str)
                except ValueError:
                    return np.nan
            
            # Handle thousands/decimal separators
            x_str = x_str.replace(',', '')
            if x_str.count('.') > 1:
                x_str = x_str.replace('.', '', x_str.count('.') - 1)
            
            try:
                float(x_str)
                return x_str
            except ValueError:
                return np.nan

        # Apply numeric cleaning and check ratio
        numeric_series = col_sample.apply(clean_numeric)
        numeric_ratio = numeric_series.notna().sum() / numeric_series.notna().count()

        if numeric_ratio >= 0.8:
            # Apply numeric cleaning to the full column
            col_numeric = col_series.apply(clean_numeric)
            # Attempt to convert to nullable integer type
            try:
                df[col] = pd.to_numeric(col_numeric, errors='raise').astype('Int64')
            except Exception:
                # If conversion to Int64 fails, convert to float
                df[col] = pd.to_numeric(col_numeric, errors='coerce')
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

        # Default to object type
        df[col] = col_series.astype('object')
        inferred_types[col] = 'object'

    return inferred_types, conversion_errors

def process_file(file_path: str, type_overrides: Optional[Dict] = None, has_headers: bool = True) -> Tuple[pd.DataFrame, Dict, Dict]:
    """
    Process the file with improved encoding handling.
    """
    try:
        file_size = os.path.getsize(file_path)
        use_chunking = file_size > 100 * 1024 * 1024  # 100 MB threshold
        
        if use_chunking:
            chunks = load_data(file_path, has_headers=has_headers, chunksize=100000)
            inferred_types = {}
            conversion_errors = {}
            df_list = []
            
            first_chunk = True
            for chunk in chunks:
                if first_chunk and not has_headers:
                    chunk.columns = [f'Column_{i+1}' for i in range(len(chunk.columns))]
                    first_chunk = False
                
                # Clean the chunk data
                for col in chunk.columns:
                    chunk[col] = chunk[col].astype(str).apply(lambda x: x.strip() if isinstance(x, str) else x)
                
                chunk_types, chunk_errors = infer_and_convert_dtypes(chunk, type_overrides)
                df_list.append(chunk)
                inferred_types.update(chunk_types)
                conversion_errors.update(chunk_errors)
            
            df = pd.concat(df_list, ignore_index=True)
        else:
            df = load_data(file_path, has_headers=has_headers)
            if not has_headers:
                df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
            
            # Clean the data
            for col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: x.strip() if isinstance(x, str) else x)
            
            inferred_types, conversion_errors = infer_and_convert_dtypes(df, type_overrides)
        
        return df, inferred_types, conversion_errors
    
    except Exception as e:
        raise ValueError(f"Error processing file: {str(e)}")

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
