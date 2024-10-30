from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
import json
from .data_type_inference import process_file_object, ProcessingMethod  # Import your inference function

class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]
    
    def __init__(self):
        super().__init__()
        self.current_df = None

    def post(self, request, format=None):
        if 'file' not in request.FILES and self.current_df is None:
            return Response({'error': 'No file provided'}, status=400)

        type_overrides = {}
        if 'type_overrides' in request.data:
            type_overrides = json.loads(request.data['type_overrides'])
        
        has_headers = request.data.get('has_headers', 'true').lower() == 'true'
        page = int(request.data.get('page', 1))
        page_size = int(request.data.get('page_size', 10))

        try:
            if 'file' in request.FILES:
                # Process new file
                file_obj = request.FILES['file']
                df, inferred_types, conversion_errors = process_file_object(
                    file_obj,
                    file_obj.name,
                    type_overrides=type_overrides,
                    has_headers=has_headers,
                    processing_method=ProcessingMethod.SPARK
                )
                self.current_df = df
            else:
                # Process existing DataFrame
                df, inferred_types, conversion_errors = process_file_object(
                    None,
                    None,
                    type_overrides=type_overrides,
                    has_headers=has_headers,
                    processing_method=ProcessingMethod.SPARK,
                    existing_df=self.current_df
                )
                self.current_df = df

            # Calculate pagination
            total_rows = len(df)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            data_preview = df.iloc[start_idx:end_idx].to_dict('records')
            
            response_data = {
                'inferred_types': inferred_types,
                'conversion_errors': conversion_errors,
                'data_preview': data_preview,
                'total_rows': total_rows,
            }
            
            if conversion_errors:
                return Response(response_data, status=400)
            
            return Response(response_data)

        except Exception as e:
            return Response({'error': str(e)}, status=400)
