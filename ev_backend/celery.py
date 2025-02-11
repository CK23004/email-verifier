import os
from celery import Celery

# Set Django settings module for celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "emailverifier.settings")

app = Celery("ev_backend")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.conf.broker_connection_retry_on_startup = True

# Auto-discover tasks from all installed apps
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
