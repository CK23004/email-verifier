from django.db import models

# Create your models here.
class UserProfile(models.Model):
    wpuser_id = models.IntegerField(unique=True)
    api_key = models.CharField(max_length=16, unique=True, null=True, blank=True)
    
    def __str__(self):
        return f"Profile of {self.user_id}"