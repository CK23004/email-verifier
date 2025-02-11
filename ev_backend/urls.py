from django.urls import path
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('', views.index, name='index'),
    path('bulk_email_verify', views.bulk_email_verify, name='bulk_email_verify'),
    path('single_email_verify', views.single_email_verify, name='single_email_verify'),
    path('find_emails', views.email_finder_view, name='email_finder_view'),
    path('create_api_key', views.create_api_key, name='create_api_key'),


    # 'process functions of apikey'

]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)



