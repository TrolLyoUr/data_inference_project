from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
import json
from .serializers import UploadedFileSerializer
from .models import UploadedFile
from .data_type_inference import process_file  # Import your inference function

class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, format=None):
        file_serializer = UploadedFileSerializer(data=request.data)
        if file_serializer.is_valid():
            file_instance = file_serializer.save()
            file_path = file_instance.file.path
            
            # Get parameters
            type_overrides = {}
            if 'type_overrides' in request.data:
                type_overrides = json.loads(request.data['type_overrides'])
            
            has_headers = request.data.get('has_headers', 'true').lower() == 'true'
            
            # Process file with parameters
            df, inferred_types, conversion_errors = process_file(
                file_path, 
                type_overrides=type_overrides,
                has_headers=has_headers
            )
            
            response_data = {
                'inferred_types': inferred_types,
                'conversion_errors': conversion_errors
            }
            
            if conversion_errors:
                return Response(response_data, status=400)
            
            return Response(response_data)
        else:
            return Response(file_serializer.errors, status=400)
