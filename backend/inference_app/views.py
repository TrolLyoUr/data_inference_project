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
            
            # Get type overrides if provided
            type_overrides = {}
            if 'type_overrides' in request.data:
                type_overrides = json.loads(request.data['type_overrides'])
            
            # Process file with type overrides
            df, inferred_types, conversion_errors = process_file(file_path, type_overrides)
            
            response_data = {
                'inferred_types': inferred_types,
                'conversion_errors': conversion_errors
            }
            
            # If there are conversion errors, return them with a 400 status
            if conversion_errors:
                return Response(response_data, status=400)
            
            return Response(response_data)
        else:
            return Response(file_serializer.errors, status=400)
