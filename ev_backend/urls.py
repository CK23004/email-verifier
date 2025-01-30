from django.urls import path
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('', views.index, name='index'),
    path('process_emails', views.process_emails, name='process_emails'),

]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)



