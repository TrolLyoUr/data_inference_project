from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
import json
from .serializers import UploadedFileSerializer
from .data_type_inference import process_file, ProcessingMethod
import pandas as pd

class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]
    current_df = None  # Make it a class variable to share across instances
    
    def post(self, request, format=None):
        file_serializer = UploadedFileSerializer(data=request.data)
        if file_serializer.is_valid():
            file_instance = file_serializer.save()
            file_path = file_instance.file.path
        
        type_overrides = {}
        if 'type_overrides' in request.data:
            type_overrides = json.loads(request.data['type_overrides'])
        
        has_headers = request.data.get('has_headers', 'true').lower() == 'true'
        page = int(request.data.get('page', 1))
        page_size = int(request.data.get('page_size', 10))

        try:
            # Process file and store DataFrame
            import time
            start_time = time.time()
            
            df, inferred_types, conversion_errors = process_file(
                file_path, 
                type_overrides=type_overrides,
                has_headers=has_headers,
                processing_method=ProcessingMethod.NATIVE_CHUNKING
            )
            
            process_time = time.time() - start_time
            print(f"Process file execution time: {process_time:.2f} seconds")

            FileUploadView.current_df = df

            # Calculate pagination
            total_rows = len(df)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            # Convert DataFrame slice to JSON
            data_preview = json.loads(
                df.iloc[start_idx:end_idx].to_json(
                    orient='records',
                    date_format='iso',
                    default_handler=str  # Handle any non-serializable objects
                )
            )
            
            response_data = {
                'inferred_types': inferred_types,
                'conversion_errors': conversion_errors,
                'data_preview': data_preview,
                'total_rows': total_rows,
            }
            print(inferred_types)
            
            if conversion_errors:
                return Response(response_data, status=400)
            
            return Response(response_data)

        except Exception as e:
            return Response({'error': str(e)}, status=400)

class DataPaginationView(APIView):
    def post(self, request, format=None):
        if FileUploadView.current_df is None:
            return Response({'error': 'No data available. Please upload a file first.'}, status=400)

        try:
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 10))
            
            df = FileUploadView.current_df
            total_rows = len(df)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            # Convert DataFrame slice to JSON
            data_preview = json.loads(
                df.iloc[start_idx:end_idx].to_json(
                    orient='records',
                    date_format='iso',
                    default_handler=str  # Handle any non-serializable objects
                )
            )
            
            return Response({
                'data_preview': data_preview,
                'total_rows': total_rows,
            })

        except Exception as e:
            return Response({'error': str(e)}, status=400)
