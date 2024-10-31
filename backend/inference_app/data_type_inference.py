from pathlib import Path
import pandas as pd
import numpy as np
import os
from dateutil.parser import parse
import re
import chardet
from typing import Optional, Tuple, Dict, Union, Literal
import codecs
from enum import Enum
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import io

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
    df_converted = pd.DataFrame()
    
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
        print(col)
        col_series = df[col]
        
        # Handle type overrides
        if col in type_overrides:
            try:
                if type_overrides[col] in ('Int64', 'float64'):
                    df_converted[col] = pd.to_numeric(col_series, errors='coerce')
                    df_converted[col].fillna('', inplace=True)
                else:
                    df_converted[col] = col_series.astype(type_overrides[col], errors='raise')
                inferred_types[col] = type_overrides[col]
                continue
            except (ValueError, TypeError) as e:
                conversion_errors[col] = {
                    'requested_type': type_overrides[col],
                    'error': str(e),
                    'sample_values': col_series.dropna().head(5).tolist()
                }
        # Replace missing values
        col_series.replace(missing_values, np.nan)
        try:
            col_series.replace(missing_value_regex, np.nan)
        except Exception:
            pass

        # Skip empty columns
        if col_series.dropna().empty:
            inferred_types[col] = 'object'
            continue

        print("Try num")
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

        sample_size = 10000 if len(col_series) > 10000 else len(col_series)
        sample_series = col_series.sample(n=sample_size, random_state=42)

        # Apply numeric cleaning and check ratio
        numeric_series = sample_series.apply(clean_numeric)
        numeric_ratio = numeric_series.notna().sum() / numeric_series.notna().count()

        if numeric_ratio >= 0.8:
            # Apply numeric cleaning to the full column
            col_numeric = col_series.apply(clean_numeric)
            # Attempt to convert to nullable integer type
            try:
                df_converted[col] = pd.to_numeric(col_numeric, errors='raise').astype('Int64')
            except Exception:
                # If conversion to Int64 fails, convert to float
                df_converted[col] = pd.to_numeric(col_numeric, errors='coerce')
            inferred_types[col] = str(df_converted[col].dtype)
            continue

        print("Try date")
        # try pandas datetime inference
        datetime_series = pd.to_datetime(sample_series, errors='coerce', format='mixed')
        if datetime_series.notna().sum() / sample_series.notna().sum() > 0.9:
            df_converted[col] = datetime_series
            inferred_types[col] = 'datetime64[ns]'
            continue

        print("Try bool")

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
        
        unique_values = set(str(x).strip().lower() for x in col_series.dropna().unique())
        if unique_values.issubset(boolean_values.keys()):
            df_converted[col] = col_series.map(lambda x: boolean_values.get(str(x).strip().lower(), np.nan))
            inferred_types[col] = 'bool'
            continue


        print("Try cate")
        
        # Check for categorical based on both unique ratio and absolute count
        unique_count = col_series.nunique(dropna=True)
        unique_ratio = unique_count / len(col_series)
        
        # Define thresholds based on dataset size with more granular ranges
        if len(col_series) < 10:
            max_unique_ratio = 0.5    # Very lenient for tiny datasets
            max_unique_count = 3     # Few distinct values for tiny datasets
        elif len(col_series) < 100:
            max_unique_ratio = 0.5    # Very lenient for tiny datasets
            max_unique_count = 10     # Few distinct values for tiny datasets
        elif len(col_series) < 1000:
            max_unique_ratio = 0.4    # Lenient for small datasets
            max_unique_count = 20     # Small threshold
        elif len(col_series) < 10000:
            max_unique_ratio = 0.15   # Moderate for medium datasets
            max_unique_count = 50     # Medium threshold
        elif len(col_series) < 100000:
            max_unique_ratio = 0.05   # Strict for large datasets
            max_unique_count = 100    # Large threshold
        elif len(col_series) < 1000000:
            max_unique_ratio = 0.02   # Very strict for very large datasets
            max_unique_count = 200    # Very large threshold
        else:
            max_unique_ratio = 0.01   # Extremely strict for massive datasets
            max_unique_count = 500    # Maximum threshold for massive datasets

        # Determine if categorical based on either criterion
        is_categorical = (unique_ratio <= max_unique_ratio) or (unique_count <= max_unique_count)
        
        if is_categorical:
            df_converted[col] = col_series.astype('category')
            inferred_types[col] = 'category'
            continue

        # Default to object type
        df_converted[col] = col_series.astype('object')
        inferred_types[col] = 'object'

    return df_converted, inferred_types, conversion_errors

class ProcessingMethod(Enum):
    NATIVE_CHUNKING = "native_chunking"
    SPARK = "spark"
    SINGLE_THREAD = "single_thread"

def create_spark_session(app_name: str = "DataTypeInference") -> Optional[SparkSession]:
    """Create or get existing Spark session with error handling"""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.broadcastTimeout", "600") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultSize", "2g") \
            .getOrCreate()
        
        # Set log level to DEBUG for more detailed output
        # spark.sparkContext.setLogLevel("DEBUG")
        
        return spark
    except Exception as e:
        print(f"Failed to create Spark session: {str(e)}")
        return None

def process_with_spark(file_path: str, 
                      spark: Optional[SparkSession] = None,
                      type_overrides: Optional[Dict] = None,
                      has_headers: bool = True) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process file using Pandas API on Spark with fallback"""
    try:
        if spark is None:
            spark = create_spark_session()

            
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            # Initialize Pandas on Spark
            ps.set_option('compute.default_index_type', 'distributed')
            
            # Read file using Pandas API on Spark
            if file_ext == '.csv':
                psdf = ps.read_csv(
                    file_path,
                    header=0 if has_headers else None
                )
            elif file_ext in ['.xls', '.xlsx']:
                psdf = pd.read_excel(file_path, header=0 if has_headers else None)
            else:
                raise ValueError("Unsupported file format for Spark processing")

            # Set column names if no headers
            if not has_headers:
                psdf.columns = [f'Column_{i+1}' for i in range(len(psdf.columns))]
            
            # Apply type inference
            inferred_types, conversion_errors = infer_and_convert_dtypes(psdf, type_overrides)
            
            return psdf, inferred_types, conversion_errors

        finally:
            # Clean up Spark resources
            if spark:
                try:
                    spark.catalog.clearCache()
                except Exception as e:
                    print(f"Error clearing Spark cache: {str(e)}")

    except Exception as e:
        print(f"Error in Spark processing: {str(e)}")

def determine_processing_method(file_path: str, 
                              requested_method: ProcessingMethod, 
                              size_threshold: int) -> ProcessingMethod:
    """Determine the most appropriate processing method based on file size and format"""
    file_size = os.path.getsize(file_path)
    file_ext = os.path.splitext(file_path)[1].lower()
    
    # For very large files, prefer Spark
    if file_size > size_threshold * 10:  # 1GB+
        return ProcessingMethod.SPARK
    
    # For medium-sized files, use the requested method or native chunking
    if file_size > size_threshold:  # 100MB+
        return requested_method if requested_method != ProcessingMethod.SINGLE_THREAD else ProcessingMethod.NATIVE_CHUNKING
    
    # For small files, use single thread unless specifically requested otherwise
    return requested_method if requested_method != ProcessingMethod.SPARK else ProcessingMethod.SINGLE_THREAD

def process_file(
    file_path: str, 
    type_overrides: Optional[Dict] = None, 
    has_headers: bool = True,
    processing_method: Union[ProcessingMethod, str] = ProcessingMethod.SINGLE_THREAD,
    spark_session: Optional[SparkSession] = None,
    chunk_size: int = 100000,
    size_threshold: int = 100 * 1024 * 1024  # 100 MB
) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process file with smart method selection and error handling"""
    # Convert string to enum if necessary
    if isinstance(processing_method, str):
        processing_method = ProcessingMethod(processing_method.lower())

    # Determine the most appropriate processing method
    # processing_method = determine_processing_method(file_path, processing_method, size_threshold)

    # Process according to determined method
    if processing_method == ProcessingMethod.SPARK:
        return process_with_spark(file_path, spark_session, type_overrides, has_headers)
    
    elif processing_method == ProcessingMethod.NATIVE_CHUNKING:
        file_ext = os.path.splitext(file_path)[1].lower()
        inferred_types = {}
        conversion_errors = {}
        
        if file_ext == '.csv':
            # First try to detect and validate encoding
            encoding = get_valid_encoding(file_path)
            
            # Common CSV reading parameters
            csv_params = {
                'encoding': encoding,
                'low_memory': False,
                'chunksize': chunk_size,
                'on_bad_lines': 'warn',  # Log warning for bad lines instead of failing
                'encoding_errors': 'ignore',  # Replace invalid characters
                'iterator': True,
                'header': 0 if has_headers else None
            }
            df_list = []
            
            for chunk in pd.read_csv(file_path, **csv_params):
                print(chunk)
                # Infer types and collect errors for this chunk
                chunk_converted, chunk_types, chunk_errors = infer_and_convert_dtypes(chunk, type_overrides)
                df_list.append(chunk_converted)
                print(df_list)
                # Merge dictionaries
                for key, value in chunk_types.items():
                    if key not in inferred_types:
                        inferred_types[key] = value
                    else:
                        # Ensure consistent data types across chunks
                        if inferred_types[key] != value:
                            inferred_types[key] = 'object'  # Fallback to object if inconsistent
                conversion_errors.update(chunk_errors)

            print("finish")
            # Concatenate all parquet files into final dataframe
            full_df = pd.concat(df_list, ignore_index=True)
            
            return full_df, inferred_types, conversion_errors
    
        elif file_ext in ['.xls', '.xlsx']:
            try:
                # Excel files don't need encoding detection
                if has_headers:
                    return pd.read_excel(file_path)
                else:
                    df = pd.read_excel(file_path, header=None)
                    inferred_types, conversion_errors = infer_and_convert_dtypes(chunk, type_overrides)
                    return df, inferred_types, conversion_errors
            except Exception as e:
                raise ValueError(f"Failed to read Excel file: {str(e)}")
        else:
            raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")
