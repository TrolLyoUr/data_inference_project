from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from .serializers import UploadedFileSerializer
from .models import UploadedFile
from .data_type_inference import infer_data_types  # Import your inference function
import pandas as pd

class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, format=None):
        file_serializer = UploadedFileSerializer(data=request.data)
        if file_serializer.is_valid():
            file_instance = file_serializer.save()
            file_path = file_instance.file.path
            # Load data
            df = pd.read_csv(file_path)  # Adjust based on file type
            # Infer data types
            df, inferred_types = infer_data_types(df)
            # Return the inferred types
            return Response({'inferred_types': inferred_types})
        else:
            return Response(file_serializer.errors, status=400)