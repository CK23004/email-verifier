from django.shortcuts import render
import requests
import csv, os 
from django.conf import settings
import io
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json, re
from .tasks import verify_emails_in_parallel
from celery.signals import task_success
import pandas as pd
import string, random
from .models import UserProfile
def index(request):
    return HttpResponse("Hello, world! This is ev_backend.")


# Constants
CONSUMER_KEY = "1a446c8347d426a691caa39aa1c98d41"
CONSUMER_SECRET = "250cc64cd5ba487dce9ba53406ea6d9f"

sender_email = "kamlesh@sphurti.net"
proxy_host = "gw-open.netnut.net"
proxy_port = 9595
proxy_user = "kamlesh007-evsh-any"
proxy_password = "Kamleshsurana@007"
@csrf_exempt
def bulk_email_verify(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=400)
    
    try:
        data = json.loads(request.body)


        wp_user_id = data.get("wp_userid", 1)
        email_count = 0
        file_name = ''
        if not wp_user_id:
            return JsonResponse({"error": "wpuserid not found"}, status=400)
        
        
        # Get file URL
        try : 
            file_data = data.get("form_data", {})
            print(1)
        except:
            pass

 
        def is_email(val): return re.match(r"[^@]+@[^@]+\.[^@]+", val)
        if file_data:
            print(file_data)
            file_info = file_data.get("15", {})  # Ensure it doesn't break if key is missing
            file_url = file_info.get("value")
            file_name = file_info.get("file_original")
            file_ext = file_info.get("ext")

            print(1)
            if file_ext.lower() != "csv":
                return JsonResponse({"error": "Uploaded file is not a CSV"}, status=400)
            
            # Fetch and read CSV
            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                return JsonResponse({"error": "Failed to download file"}, status=500)
            csv_content = file_response.content.decode("utf-8")
            print(2)
            csv_file = io.StringIO(csv_content)

            # Read CSV content as a string, validate emails
            email_list = []
            for row in csv_file:
                email = row.strip()  # Remove any leading/trailing whitespace
                if is_email(email):
                    email_list.append(email)            
            email_count = len(email_list)
            # email_count = sum(1 for row in csv_reader if row) if is_email(first_row[0]) else sum(1 for row in csv_reader if row)
        else:
            file_name = "Untitled.csv"
            email_string = data.get("form_data", {}).get("8", {}).get("value", "")
            common_email_list = [email.strip() for email in email_string.split('\r\n')]
            email_list = [email for email in common_email_list if is_email(email)]
            email_count = len(email_list)

       

        wallet_action(wp_user_id, action="debit", email_count=email_count, file_name=file_name)

        
        # Fetch post_id from webhook data
        post_id = data.get("post_id", {})
        if email_list:
            if file_name:
                base_file_name = os.path.splitext(file_name)[0]
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"{base_file_name}_{wp_user_id}{post_id}.csv") 
            else:
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"Untitled_{wp_user_id}{post_id}.csv") 
                
            # Trigger the Celery task
           
            verify_emails_in_parallel.delay(sender_email, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type = 'bulk_verify', wpuser_id = wp_user_id, post_id=post_id, output_file_name=output_file_name)

            # Output to confirm the task is triggered
            print("Celery task triggered successfully!")
        else:
            return JsonResponse({"error": f"Failed to Process Emails. No valid Emails Found."}, status=500)
        if not post_id:
            return JsonResponse({"error": "Post ID not found"}, status=400)
        
        return JsonResponse({"success": True, "credits_used": email_count})
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)



@csrf_exempt
def single_email_verify(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=400)
    
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            post_id = data.get("post_id")
            form_data = data.get("form_data", {})
            wp_user_id = data.get("wp_user_id", 1)
            email_id = None
            for key, field in form_data.items():
                if field.get("type") == "email":
                    email_id = field.get("value")
                    break
            
            if not email_id:
                return JsonResponse({"error": "No email found in form_data"}, status=400)
            
            wallet_action(wp_user_id, action="debit", email_count=1, file_name="Single Email Verify")
            email_list = []
            email_list.append(email_id)
            print(email_list)
            # Trigger Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list, service_type='single_verify', 
                wpuser_id=wp_user_id, post_id=post_id, 
                output_file_name=None
            )
            
            return JsonResponse({"message": "Verification task triggered successfully"})
        
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    
    return JsonResponse({"error": "Invalid request method"}, status=405)
        

@csrf_exempt  # Use this if you are testing without CSRF tokens
def email_finder_view(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            post_id = data.get("post_id")
            wp_user_id = data.get("wp_user_id")
            form_data = data.get("form_data", {})
            
            person_name = None
            company_name = None
            
            for key, field in form_data.items():
                if field.get("type") == "name":
                    person_name = field.get("value")
                elif field.get("type") == "text":
                    company_name = field.get("value")
            
            if not person_name or not company_name:
                return JsonResponse({"error": "Missing required fields"}, status=400)
            
            first_name, *last_name_parts = person_name.split()
            last_name = last_name_parts[-1] if last_name_parts else ""
            
            company_domain = company_name

            # Generate email patterns
            email_list = [
                f"{first_name}@{company_domain}",
                f"{last_name}@{company_domain}",
                f"{first_name}{last_name}@{company_domain}",
                f"{first_name}.{last_name}@{company_domain}",
                f"{first_name[0]}{last_name}@{company_domain}",
                f"{first_name[0]}.{last_name}@{company_domain}",
                f"{first_name}{last_name[0]}@{company_domain}",
                f"{first_name}.{last_name[0]}@{company_domain}",
                f"{first_name[0]}{last_name[0]}@{company_domain}",

            ]

            # Remove duplicates and ensure validity
            email_list = list(set(email_list))

            # Call Celery task with the properly formatted email list
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list=email_list, service_type='email_finder', 
                wpuser_id=wp_user_id, post_id=post_id, output_file_name=None
            )

            
            return JsonResponse({"message": "Verification task triggered successfully"})
        
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    
    return JsonResponse({"error": "Invalid request method"}, status=405)


def generate_token(length=32):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

@csrf_exempt
def create_api_key(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            user_id = data.get("user_id")
            
            if not user_id:
                return JsonResponse({"error": "User ID is required"}, status=400)
            
            token = generate_token()
            
            # Store in user profile
            user_profile, created = UserProfile.objects.get_or_create(wpuser_id=user_id)
            user_profile.api_key = token
            user_profile.save()
            
            # Update WordPress usermeta
            url = "https://insideemails.com/?wpwhpro_action=wp-modify-usermeta&wpwhpro_api_key=5zvkfisvqtt2y6fxwcnfrvgczcyt9jeatvyo5e2l7zludi2ps7z6crtkd0vr6lyk&action=wp_manage_user_meta_data"
            payload = {
                "user_id": user_id,
                "meta_update": {"api_key": token}
            }
            response = requests.post(url, json=payload)
            
            return JsonResponse({"message": "API key created successfully", "api_key": token})
        
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    
    return JsonResponse({"error": "Invalid request method"}, status=405)




def wallet_action(wp_user_id, action, email_count, file_name):
    wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
    wallet_response = requests.get(wallet_url)
    wallet_credits = float(wallet_response.json())
    if wallet_response.status_code != 200:
        return JsonResponse({"error": "Failed to fetch wallet balance"}, status=500)
    if action == 'debit':
        if email_count <= wallet_credits and email_count > 0:
                # Debit wallet
                debit_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}"
                debit_payload = {
                    "amount": email_count,
                    "action": f"{action}",
                    "consumer_key": CONSUMER_KEY,
                    "consumer_secret": CONSUMER_SECRET,
                    "transaction_detail": f"debit through {file_name}",
                    "payment_method" : "Auto Deduct through Utilisation"
                }
                
                headers = {"Content-Type": "application/json"}
                debit_response = requests.put(debit_url, headers=headers, json=debit_payload)
                
                if debit_response.status_code == 200:
                    return True
                else:
                    # Log or return specific error details from the response
                    return False
            