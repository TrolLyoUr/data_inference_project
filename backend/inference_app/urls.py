from django.urls import path
from .views import FileUploadView, DataPaginationView

urlpatterns = [
    path('upload/', FileUploadView.as_view(), name='file-upload'),
    path('paginate/', DataPaginationView.as_view(), name='data-paginate'),
]