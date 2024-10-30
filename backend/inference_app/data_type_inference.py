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
                    df[col].fillna('', inplace=True)
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
            df[col] = col_series.astype('category')
            inferred_types[col] = 'category'
            continue

        # Default to object type
        df[col] = col_series.astype('object')
        inferred_types[col] = 'object'

    return inferred_types, conversion_errors

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
        
        if spark is None:
            # Fallback to native chunking if Spark is unavailable
            print("Spark unavailable, falling back to native chunking")
            chunks = load_data(file_path, has_headers=has_headers, chunksize=100000)
            return process_chunks(chunks, has_headers, type_overrides)
            
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
                pdf = pd.read_excel(file_path, header=0 if has_headers else None)
                psdf = ps.DataFrame(pdf)
            else:
                raise ValueError("Unsupported file format for Spark processing")

            # Set column names if no headers
            if not has_headers:
                psdf.columns = [f'Column_{i+1}' for i in range(len(psdf.columns))]

            # Clean the data using Pandas API on Spark
            for col in psdf.columns:
                if psdf[col].dtype == 'object':
                    psdf[col] = psdf[col].str.strip()

            # Convert to pandas for type inference
            pdf = psdf.to_pandas()
            
            # Apply type inference
            inferred_types, conversion_errors = infer_and_convert_dtypes(pdf, type_overrides)
            
            return pdf, inferred_types, conversion_errors

        finally:
            # Clean up Spark resources
            if spark:
                try:
                    spark.catalog.clearCache()
                except Exception as e:
                    print(f"Error clearing Spark cache: {str(e)}")

    except Exception as e:
        print(f"Error in Spark processing: {str(e)}")
        # Fallback to native chunking
        chunks = load_data(file_path, has_headers=has_headers, chunksize=100000)
        return process_chunks(chunks, has_headers, type_overrides)

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
    file_path: str = None, 
    type_overrides: Optional[Dict] = None, 
    has_headers: bool = True,
    processing_method: Union[ProcessingMethod, str] = ProcessingMethod.SINGLE_THREAD,
    spark_session: Optional[SparkSession] = None,
    chunk_size: int = 100000,
    size_threshold: int = 100 * 1024 * 1024,  # 100 MB
    existing_df: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process file or existing DataFrame with smart method selection and error handling"""
    try:
        # Convert string to enum if necessary
        if isinstance(processing_method, str):
            processing_method = ProcessingMethod(processing_method.lower())

        # If existing DataFrame is provided, use it directly
        if existing_df is not None:
            df = existing_df.copy()
            print(df)
            
            if processing_method == ProcessingMethod.SPARK:
                # Convert to Spark DataFrame if needed
                try:
                    if spark_session is None:
                        spark_session = create_spark_session()
                    if spark_session is not None:
                        psdf = ps.DataFrame(df)
                        pdf = psdf.to_pandas()
                        inferred_types, conversion_errors = infer_and_convert_dtypes(pdf, type_overrides)
                        return pdf, inferred_types, conversion_errors
                except Exception as e:
                    print(f"Error in Spark processing: {str(e)}")
            
            # If not using Spark or Spark failed, process directly
            if len(df) > chunk_size and processing_method == ProcessingMethod.NATIVE_CHUNKING:
                # Split DataFrame into chunks
                chunk_dfs = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
                return process_chunks(chunk_dfs, has_headers, type_overrides)
            else:
                inferred_types, conversion_errors = infer_and_convert_dtypes(df, type_overrides)
                return df, inferred_types, conversion_errors
        
        # If no existing DataFrame, process from file
        if file_path is None:
            raise ValueError("Either file_path or existing_df must be provided")
            
        # Original file processing logic
        if processing_method == ProcessingMethod.SPARK:
            return process_with_spark(file_path, spark_session, type_overrides, has_headers)
        elif processing_method == ProcessingMethod.NATIVE_CHUNKING:
            chunks = load_data(file_path, has_headers=has_headers, chunksize=chunk_size)
            return process_chunks(chunks, has_headers, type_overrides)
        else:  # SINGLE_THREAD
            return process_single_thread(file_path, has_headers, type_overrides)

    except Exception as e:
        if processing_method == ProcessingMethod.SPARK:
            print(f"Spark processing failed, falling back to native chunking: {str(e)}")
            if existing_df is not None:
                chunk_dfs = [existing_df[i:i + chunk_size] for i in range(0, len(existing_df), chunk_size)]
                return process_chunks(chunk_dfs, has_headers, type_overrides)
            else:
                chunks = load_data(file_path, has_headers=has_headers, chunksize=chunk_size)
                return process_chunks(chunks, has_headers, type_overrides)
        raise ValueError(f"Error processing data: {str(e)}")

def process_single_thread(file_path: str, 
                         has_headers: bool, 
                         type_overrides: Optional[Dict]) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process file in single thread mode"""
    df = load_data(file_path, has_headers=has_headers)
    if not has_headers:
        df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
    
    # Clean the data
    for col in df.columns:
        df[col] = df[col].astype(str).apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    inferred_types, conversion_errors = infer_and_convert_dtypes(df, type_overrides)
    return df, inferred_types, conversion_errors

def process_chunks(chunks, has_headers: bool, type_overrides: Optional[Dict]) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process data in chunks"""
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
    
    return pd.concat(df_list, ignore_index=True), inferred_types, conversion_errors

def process_file_object(
    file_obj,
    file_name: str,
    type_overrides: Optional[Dict] = None,
    has_headers: bool = True,
    processing_method: Union[ProcessingMethod, str] = ProcessingMethod.SINGLE_THREAD,
    spark_session: Optional[SparkSession] = None,
    chunk_size: int = 100000
) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Process file directly from file object"""
    try:
        file_extension = file_name.split('.')[-1].lower()
        file_content = file_obj.read()
        
        if file_extension == 'csv':
            df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
        elif file_extension in ['xls', 'xlsx']:
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            raise ValueError("Unsupported file format")

        if not has_headers:
            df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]

        # Clean the data
        for col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: x.strip() if isinstance(x, str) else x)

        if processing_method == ProcessingMethod.SPARK:
            try:
                if spark_session is None:
                    spark_session = create_spark_session()
                if spark_session is not None:
                    psdf = ps.DataFrame(df)
                    df = psdf.to_pandas()
            except Exception as e:
                print(f"Spark processing failed: {str(e)}")

        if len(df) > chunk_size and processing_method == ProcessingMethod.NATIVE_CHUNKING:
            chunk_dfs = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            return process_chunks(chunk_dfs, has_headers, type_overrides)
        
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
