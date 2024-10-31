# Data Type Inference Project

A robust data processing application that automatically infers and converts data types from CSV and Excel files, with support for large-scale data processing using multiple methods.

## Features

- Automatic data type inference for:
  - Numeric values (including currency and percentages)
  - Boolean values
  - Dates and timestamps
  - Categorical data
  - Text/object data
- Multiple processing methods:
  - Single-thread processing for small files
  - Native chunking for medium-sized files
  - PySpark integration for large-scale data
- Comprehensive encoding detection and handling
- Support for CSV and Excel files
- REST API endpoints for file upload and data pagination
- React-based frontend with data preview and type modification capabilities

## Prerequisites

- Python 3.8+
- Node.js 14+
- Java 8+ (for Spark processing) (TODO)

## Installation

### Backend Setup

1. Create and activate a virtual environment:
   python -m venv venv
   source venv/bin/activate # On Windows: venv\Scripts\activate

2. Install Python dependencies:
   cd backend
   pip install -r requirements.txt

3. Run Django migrations:
   python manage.py migrate

### Frontend Setup

1. Install Node.js dependencies:
   cd frontend/data-inference-frontend
   npm install

## Running the Application

1. Start the Django backend server:
   cd backend
   python manage.py runserver

2. Start the React frontend development server:
   cd frontend/data-inference-frontend
   npm start

The application will be available at:

- Frontend: http://localhost:3000
- Backend API: http://localhost:8000

## API Endpoints

### File Upload

- `POST /api/upload/`
  - Parameters:
    - `file`: The file to process (CSV or Excel)
    - `type_overrides`: JSON object of column name to desired type mappings
    - `has_headers`: Boolean indicating if file has headers
    - `page`: Page number for data preview
    - `page_size`: Number of rows per page

### Data Pagination

- `POST /api/paginate/`
  - Parameters:
    - `page`: Page number
    - `page_size`: Number of rows per page

## Type Inference Logic

The application uses a sophisticated type inference algorithm that:

1. Handles missing values and various data formats
2. Detects boolean values with multiple representations
3. Identifies categorical data based on unique value ratios
4. Processes numeric data with currency and percentage handling
5. Supports datetime inference with multiple formats

For details, see:
python:backend/inference_app/data_type_inference.py
startLine: 100
endLine: 289

## Error Handling

The application includes comprehensive error handling:

- File encoding detection and validation
- Type conversion errors with detailed feedback
- Processing method fallbacks
- API error responses with meaningful messages

## Performance Considerations

- Uses chunked processing for large CSV files
- Implements Spark processing for very large datasets (TODO)
- Supports pagination for efficient data preview
- Includes memory optimization techniques

## License

This project is licensed under the MIT License - see the LICENSE file for details.
