from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
import json
import pandas as pd
import os
from .serializers import UploadedFileSerializer
from .models import UploadedFile
from .data_type_inference import process_file  # Import your inference function

class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]
    
    def __init__(self):
        super().__init__()
        self.current_df = None
        self.file_path = None

    def post(self, request, format=None):
        file_serializer = UploadedFileSerializer(data=request.data)
        if file_serializer.is_valid():
            file_instance = file_serializer.save()
            self.file_path = file_instance.file.path
            
            type_overrides = {}
            if 'type_overrides' in request.data:
                type_overrides = json.loads(request.data['type_overrides'])
            
            has_headers = request.data.get('has_headers', 'true').lower() == 'true'
            page = int(request.data.get('page', 1))
            page_size = int(request.data.get('page_size', 10))
            
            # Process file and store DataFrame
            df, inferred_types, conversion_errors = process_file(
                self.file_path, 
                type_overrides=type_overrides,
                has_headers=has_headers
            )
            self.current_df = df
            
            # Save processed DataFrame
            processed_path = f"{os.path.splitext(self.file_path)[0]}_processed.csv"
            df.to_csv(processed_path, index=False)
            
            # Calculate pagination
            total_rows = len(df)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            data_preview = df.iloc[start_idx:end_idx].to_dict('records')
            print(data_preview)
            
            response_data = {
                'inferred_types': inferred_types,
                'conversion_errors': conversion_errors,
                'data_preview': data_preview,
                'total_rows': total_rows
            }
            
            if conversion_errors:
                return Response(response_data, status=400)
            
            return Response(response_data)
        else:
            return Response(file_serializer.errors, status=400)
