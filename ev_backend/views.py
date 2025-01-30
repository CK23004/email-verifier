from django.shortcuts import render
import requests
import csv, os 
from django.conf import settings
import io
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json, re
from .tasks import process_email_list
from celery.signals import task_success
import pandas as pd
def index(request):
    return HttpResponse("Hello, world! This is ev_backend.")


# Constants
CONSUMER_KEY = "1a446c8347d426a691caa39aa1c98d41"
CONSUMER_SECRET = "250cc64cd5ba487dce9ba53406ea6d9f"

@csrf_exempt
def process_emails(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=400)
    
    try:
        data = json.loads(request.body)
        wp_user_id = data.get("form_data", {}).get("12", {}).get("value")
        
        if not wp_user_id:
            return JsonResponse({"error": "wpuserid not found"}, status=400)
        
        # Fetch wallet balance
        wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
        wallet_response = requests.get(wallet_url)
        wallet_credits = float(wallet_response.json())
        if wallet_response.status_code != 200:
            return JsonResponse({"error": "Failed to fetch wallet balance"}, status=500)
        
        # Get file URL
        file_data = data.get("form_data", {}).get("1", {}).get("value_raw", [])
        file_url = file_data[0].get("value")
        file_name = file_data[0].get("file_original")
        file_ext = file_data[0].get("ext")
        # if not file_data:
        #     return JsonResponse({"error": "No file uploaded"}, status=400)
        def is_email(val): return re.match(r"[^@]+@[^@]+\.[^@]+", val)
        if file_url:
            
            if file_ext.lower() != "csv":
                return JsonResponse({"error": "Uploaded file is not a CSV"}, status=400)
            
            # Fetch and read CSV
            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                return JsonResponse({"error": "Failed to download file"}, status=500)
            csv_content = file_response.content.decode("utf-8")
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
            email_string = data.get("form_data", {}).get("8", {}).get("value", [])
            common_email_list = [email.strip() for email in email_string.split('\r\n')]
            email_list = [email for email in email_list if is_email(email)]
            email_count = len(email_list)

       

        debit_successfull = False
        if email_count <= wallet_credits and email_count > 0:
            # Debit wallet
            debit_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}"
            debit_payload = {
                "amount": email_count,
                "action": "debit",
                "consumer_key": CONSUMER_KEY,
                "consumer_secret": CONSUMER_SECRET,
                "transaction_detail": f"debit through {file_name}",
                "payment_method" : "API Deduct"
            }
            
            headers = {"Content-Type": "application/json"}
            debit_response = requests.put(debit_url, headers=headers, json=debit_payload)
            
            if debit_response.status_code == 200:
                debit_successfull = True
            else:
                # Log or return specific error details from the response
                return JsonResponse({"error": f"Failed to debit wallet. Response: {debit_response.text}"}, status=500)
        
        
        # Fetch post_id from webhook data
        post_id = data.get("post_id", {})
        if email_list:
            # Assuming you have the post_id and user_id (replace with actual values)
            # post_id = 123  # Example post ID
            # user_id = 456  # Example user ID
            if file_name:
                base_file_name = os.path.splitext(file_name)[0]
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"{base_file_name}_{wp_user_id}_{post_id}.csv") 
                 
            else:
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"Untitled_{wp_user_id}_{post_id}.csv") 
                
            # Trigger the Celery task
            process_email_list.delay(email_list, post_id, wp_user_id, output_file_name, email_count)

            # Output to confirm the task is triggered
            print("Celery task triggered successfully!")
        else:
            return JsonResponse({"error": f"Failed to Process Emails. No valid Emails Found."}, status=500)
        if not post_id:
            return JsonResponse({"error": "Post ID not found"}, status=400)
        
        # Prepare the payload for the post-update request
        post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
        if debit_successfull:
            post_update_payload = {
                "post_id": post_id,
                "post_content": f"<p>Credits Utilized: {email_count}<br> <a href='#'>Processing File</a></p>",
                "post_title": file_name
            }
        else: 
            post_update_payload = {
                "post_id": post_id,
                "post_content": f"<p>Credits Required: {email_count}<br>Failed: Insufficient Credits</p>",
                "post_title": file_name
            }

        # Send the post update request
        post_update_response = requests.post(post_update_url, json=post_update_payload)

        if post_update_response.status_code != 200:
            return JsonResponse({"error": "Failed to update post"}, status=500)
        
        return JsonResponse({"success": True, "credits_used": email_count})
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)



def task_result(result, **kwargs):
    if 'error' in result:
        print(f"Task failed with error: {result['error']}")
    else:
        try:
            print(f"Task completed successfully. output_file: {result['output_file']}, post_id: {result['post_id']}, user_id {result['user_id']}")
            post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
            file_url = settings.BASE_URL + settings.MEDIA_URL + os.path.basename(result["output_file"])
            post_update_payload = {
                    "post_id": result['post_id'],
                    "post_content": f"<p>Credits Utilized: { result['email_count']}<br> <a href='{file_url}'>Download File</a></p>",
                }
            

            # Send the post update request
            post_update_response = requests.post(post_update_url, json=post_update_payload)

            if post_update_response.status_code != 200:
                return JsonResponse({"error": "Failed to update post"}, status=500)
            
        except Exception as e:
            pass
        # Trigger additional logic here (e.g., notify user, update database, etc.)

task_success.connect(task_result , sender=process_email_list)