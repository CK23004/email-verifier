from celery import Celery, group, chord
from celery.signals import worker_ready
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib, threading
import socks
import aiodns, aiohttp
import asyncio
import socket
import logging
import time, sys
import pandas as pd
import random
import logging
from celery import shared_task

if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# Initialize Celery

# Cache for MX records
dns_cache = {}
no_mx_domains = set()
catch_all_domains = set()
batch_tasks = {}




async def update_frontend_async(progress, batch_id):
    """Send progress updates asynchronously to the frontend."""
    post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
    data = {"batch_id": batch_id, "progress": progress}
    if batch_tasks[batch_id]['service_type'] == 'bulk_verify':

        if progress == 100:
            valid_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Valid")
            invalid_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Invalid")
            spamBlock_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Spam Block")
            risky_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Catch All")
            unknown_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Unknown")
            tempLimited_count = sum(1 for status in batch_tasks[batch_id]['results'].values() if status == "Temporary Limited")

            
            df = pd.DataFrame(list(batch_tasks[batch_id]['results'].items()), columns=["Email", "Status"])
            # Save DataFrame to CSV
            output_file_name =  batch_tasks[batch_id]['output_file_name']
            df.to_csv(output_file_name, index=False)
            output_file_url = f'https://verifier.insideemails.com/media/{output_file_name}'
            import os
            file_path = os.path.join(os.path.dirname(__file__), "html1.php")
            with open(file_path, "r", encoding="utf-8") as file:
                html_content = file.read()
            

            # html_content = html_content.replace("{{ deliverable_count }}", str(valid_count))
            # html_content = html_content.replace("{{ undeliverable_count }}", str(invalid_count))
            # html_content = html_content.replace("{{ risky_count }}", str(risky_count))
            # html_content = html_content.replace("{{ unknown_count }}",str (unknown_count))
            # html_content = html_content.replace("{{ spamBlock_count }}", str(spamBlock_count))
            # html_content = html_content.replace("{{ tempLimited_count }}", str(tempLimited_count))
            # html_content = html_content.replace("{{ output_file_name }}", str(output_file_name))
            # html_content = html_content.replace("{{ output_file_url }}", str(output_file_url))
            # html_content = html_content.replace("{{ email_count }}", str(batch_tasks[batch_id]['initial_count']))
            

            post_update_payload = {
            
                           "post_id": batch_tasks[batch_id]['post_id'],
                            "post_content": f'<div id="status" class="d-none">{batch_tasks[batch_id]["status"]}</div><div id="email_count" class="d-none">{batch_tasks[batch_id]["initial_count"]}</div><div id="progress" class="d-none">{progress}</div><br> [email_verification_report output_file_url={output_file_url}  deliverable_count={valid_count} undeliverable_count={invalid_count} risky_count= {risky_count} unknown_count = {unknown_count} spamBlock_count = {spamBlock_count} tempLimited_count = {tempLimited_count} ]',
                        }
        else: 
            post_update_payload = {
            
                            "post_id": batch_tasks[batch_id]['post_id'],
                            "post_content": f'''<div id="status" class="d-none">{batch_tasks[batch_id]['status']}</div>
                                                <div id="email_count" class="d-none">{batch_tasks[batch_id]['initial_count']}</div>
                                                <div id="progress" class="d-none">{progress}</div>
                                                <br> <a href='#' class="d-none"></a>''',
                        }
    
    if batch_tasks[batch_id]['service_type'] == 'single_verify':
        if progress == 100:
            # value = next(iter(batch_tasks[batch_id]['results'].values()))
            value = next(iter(batch_tasks[batch_id]['results'].values()), None)  # Prevent StopIteration
            if value is None:
                print(None)
            post_update_payload = {
            
                            "post_id": batch_tasks[batch_id]['post_id'],
                            "post_content": f'''<div id="status" class="d-none">{value}</div>
                                                <div id="progress" class="d-none">{progress}</div>''',
                        }
        else:
            return
    
    if batch_tasks[batch_id]['service_type'] == 'email_finder':
        from .views import wallet_action
        if progress == 100:
            valid_emails_found = [email for email, status in batch_tasks[batch_id]['results'].items() if status == "Valid"]
            print(f"valid_emails_found {valid_emails_found}")
            wallet_action(wp_user_id= batch_tasks[batch_id]['wpuser_id'], action="debit", email_count=len(valid_emails_found), file_name="Email Finder")
    
            post_update_payload = {
            
                            "post_id": batch_tasks[batch_id]['post_id'],
                            "post_content": f'''<div id="status" class="d-none">Completed</div>
                                                <div id="progress" class="d-none">{progress}</div>
                                               <div id="emails">{", ".join(valid_emails_found)}</div>''',
                        }
        else:
            post_update_payload = {
            
                            "post_id": batch_tasks[batch_id]['post_id'],
                            "post_content": f'''<div id="status" class="d-none">Processing</div>
                                                <div id="progress" class="d-none">{progress}</div>
                                               <div id="emails"></div>''',
                        }
            
            
            
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(post_update_url, json=post_update_payload, timeout=5) as response:
                print('request sent')
                if response.status != 200:
                    logging.ERROR(f"Frontend update failed: {response.status}")
        except Exception as e:
            logging.ERROR(f"Error updating frontend: {e}")

def generate_random_triggers():
    """Generate random trigger points between 1 and 100, ensuring 100 is always included."""
    random_points = sorted(random.sample(range(1, 100), random.randint(30, 40)))  # Random 3-7 points
    random_points.append(100)  # Ensure 100% is always triggered
    return set(random_points)




async def fetch_mx_records_async(domain):
    if domain in dns_cache:
        return dns_cache[domain]
    resolver = aiodns.DNSResolver(timeout=10)
    try:
        answers = await resolver.query(domain, 'MX')
        mx_records = sorted([(record.priority, record.host) for record in answers], key=lambda x: x[0])
        dns_cache[domain] = [record[1] for record in mx_records]
        return dns_cache[domain]
    except:
        return []


def fetch_mx_records(domain):
    try:
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(fetch_mx_records_async(domain))
        return loop.run_until_complete(future)
    except RuntimeError:
        return asyncio.run(fetch_mx_records_async(domain))


class ProxySMTP(smtplib.SMTP):
    def __init__(self, host='', port=0, proxy_host=None, proxy_port=None, proxy_user=None, proxy_password=None, *args, **kwargs):
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_password = proxy_password
        super().__init__(host, port, *args, **kwargs)
    
    def _get_socket(self, host, port, timeout):
        sock = socks.socksocket()
        sock.set_proxy(socks.SOCKS5, self.proxy_host, self.proxy_port, username=self.proxy_user, password=self.proxy_password)
        sock.settimeout(timeout)
        sock.connect((host, port))
        return sock

@shared_task
def verify_email_via_smtp( sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password, batch_id,total_emails ):
    domain = recipient.split('@')[1]
    if domain in no_mx_domains:
        status = "No MX Records Found"
    
    mx_records = fetch_mx_records(domain)
    if not mx_records:
        no_mx_domains.add(domain)
        status = "No MX Records Found"
    else:
        smtp_server = mx_records[0]
        smtp_port = 25
        
        try:
            with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:
                server.ehlo()
                server.mail(sender)
                code, message = server.rcpt(recipient)
                server_message = message.decode('utf-8')

                if code in [250, 251, 252]:
                    status = "Valid"
                elif code in [550, 551, 552, 553, 554]:
                    if 'spam policy' in server_message.lower() or 'blocked using spamhaus' in server_message.lower():
                        status = "Spam Block"
                    status =  "Invalid"
                elif code in [450, 451, 452, 421]:
                    status =  "Temporarily Limited"
                else:
                    status = "Unknown"
        except socket.timeout:
            status = "Unknown"
        except Exception as e:
            status = "Unknown"
    
    # Track progress
    batch_tasks[batch_id]["completed"] += 1
    progress = int((batch_tasks[batch_id]["completed"] / total_emails) * 100)
    # if batch_tasks[batch_id]['service_type'] == 'single_verify':
    batch_tasks[batch_id]["results"][recipient] = status

    if progress <= 25 and batch_tasks[batch_id]["part1"]:
        part1_triggers = batch_tasks[batch_id]["part1"]  # Sort for consistent order
        part_size = len(part1_triggers)

        trigger_intervals = [int(i * (25 / part_size)) for i in range(1, part_size + 1)]

        while part1_triggers and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part1_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part1"].remove(trigger_point)
            except:
                pass

    
    if 26 <= progress <= 50 and batch_tasks[batch_id]["part2"]:
        part2_triggers = batch_tasks[batch_id]["part2"] # Sort trigger points
        part_size = len(part2_triggers)

        trigger_intervals = [26 + int(i * ((50 - 26) / part_size)) for i in range(1, part_size + 1)]
        while part2_triggers and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part2_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part2"].remove(trigger_point)
            except:
                pass

    
    if 51 <= progress <= 75 and batch_tasks[batch_id]["part3"]:
        part3_triggers = batch_tasks[batch_id]["part3"]  # Sort trigger points
        part_size = len(part3_triggers)

        trigger_intervals = [51 + int(i * ((75 - 51) / part_size)) for i in range(1, part_size + 1)]
        while part3_triggers  and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part3_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part3"].remove(trigger_point)
            except:
                pass

    
    if 76 <= progress <= 99  and batch_tasks[batch_id]["part4"]:
        part4_triggers = batch_tasks[batch_id]["part4"]  # Sort trigger points
        part_size = len(part4_triggers)

        trigger_intervals = [76 + int(i * ((99 - 76) / part_size)) for i in range(1, part_size + 1)]
        while part4_triggers and trigger_intervals and  progress >= trigger_intervals[0]:
            trigger_point = min(part4_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part4"].remove(trigger_point)
            except:
                pass

    return {recipient: status}

@shared_task()
def verify_email_for_catchall(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password,batch_id, total_emails):
    domain = recipient.split('@')[1]
    if domain in catch_all_domains:
        status = "Catch All"
    else:
        mx_records = fetch_mx_records(domain)
        smtp_server = mx_records[0]
        smtp_port = 25
        invalid_email = f"nonexistent@{domain}"
        
        try:
            with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:
                server.ehlo()
                server.mail(sender)
                code, message = server.rcpt(invalid_email)
                if code in [250, 251, 252]:
                    catch_all_domains.add(domain)
                    status = "Catch All"
                status = "Valid"
        except:
            status = "Valid"
    
      # Track progress
    batch_tasks[batch_id]["completed"] += 1
    progress = int((batch_tasks[batch_id]["completed"] / total_emails) * 100)

    if progress <= 25 and batch_tasks[batch_id]["part1"]:
        part1_triggers = batch_tasks[batch_id]["part1"]  # Sort for consistent order
        part_size = len(part1_triggers)

        trigger_intervals = [int(i * (25 / part_size)) for i in range(1, part_size + 1)]

        while part1_triggers and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part1_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part1"].remove(trigger_point)
            except:
                pass

    
    if 26 <= progress <= 50 and batch_tasks[batch_id]["part2"]:
        part2_triggers = batch_tasks[batch_id]["part2"] # Sort trigger points
        part_size = len(part2_triggers)

        trigger_intervals = [26 + int(i * ((50 - 26) / part_size)) for i in range(1, part_size + 1)]
        while part2_triggers and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part2_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part2"].remove(trigger_point)
            except:
                pass

    
    if 51 <= progress <= 75 and batch_tasks[batch_id]["part3"]:
        part3_triggers = batch_tasks[batch_id]["part3"]  # Sort trigger points
        part_size = len(part3_triggers)

        trigger_intervals = [51 + int(i * ((75 - 51) / part_size)) for i in range(1, part_size + 1)]
        while part3_triggers  and trigger_intervals and progress >= trigger_intervals[0]:
            trigger_point = min(part3_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part3"].remove(trigger_point)
            except:
                pass

    
    if 76 <= progress <= 99  and batch_tasks[batch_id]["part4"]:
        part4_triggers = batch_tasks[batch_id]["part4"]  # Sort trigger points
        part_size = len(part4_triggers)

        trigger_intervals = [76 + int(i * ((99 - 76) / part_size)) for i in range(1, part_size + 1)]
        while part4_triggers and trigger_intervals and  progress >= trigger_intervals[0]:
            trigger_point = min(part4_triggers)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
            loop.close()
            trigger_intervals.pop(0)
            try:
                batch_tasks[batch_id]["part4"].remove(trigger_point)
            except:
                pass

    return {recipient: status}




@shared_task
def process_first_round_results(first_round_results_list, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, second_quarter_parts, third_quarter_parts):
    """Processes first-round results and triggers spam-block retry."""
    batch_tasks[batch_id]["completed"] += len(first_round_results_list)
    first_round_results = {}
    for result in first_round_results_list:
        first_round_results.update(result)
    # Find Spam Blocked Emails
    spam_blocked_emails = [email for email, status in first_round_results.items() if status == "Spam Block"]

    if spam_blocked_emails:
        total_emails = len(spam_blocked_emails)
        batch_tasks[batch_id].update({
            "total": total_emails,
            "completed": 0,
            "status": "processing",
            "part1": second_quarter_parts[0],
            "part2": second_quarter_parts[1],
            "part3": second_quarter_parts[2],
            "part4": second_quarter_parts[3],
        })
        batch_tasks[batch_id]["spam_block_retries"] = len(spam_blocked_emails)

        retry_tasks = group(
            verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails) 
            for email in spam_blocked_emails
        )
        return chord(retry_tasks)(process_spam_block_results.s(first_round_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts))
    else:
        return process_spam_block_results(first_round_results, {}, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts)

@shared_task
def process_spam_block_results(first_round_results_list, retry_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts):
    """Merges spam-block retry results and triggers valid email processing."""
    # Update first_round_results with retried statuses
    if isinstance(first_round_results_list, list):  # More robust type check
        first_round_results = {}
        for result in first_round_results_list:
            first_round_results.update(result)
    else:
        first_round_results = first_round_results_list

    for email, new_status in retry_results.items():
                if email in first_round_results:  #  Only update existing keys
                    first_round_results[email] = new_status 

    # Identify Valid Emails
    valid_emails = [email for email, status in first_round_results.items() if status == "Valid"]

    if valid_emails:
        total_emails = len(valid_emails)
        batch_tasks[batch_id].update({
            "total": total_emails,
            "completed": 0,
            "status": "processing",
            "part1": third_quarter_parts[0],
            "part2": third_quarter_parts[1],
            "part3": third_quarter_parts[2],
            "part4": third_quarter_parts[3],
        })
        batch_tasks[batch_id]["valid_email_retries"] = len(valid_emails)

        catch_all_tasks = group(
            verify_email_for_catchall.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails) 
            for email in valid_emails
        )
        return chord(catch_all_tasks)(finalize_results.s(first_round_results, batch_id))
    else:
        return finalize_results(first_round_results, {}, batch_id)

@shared_task
def finalize_results(first_round_results_list, catch_all_results, batch_id):

    """Final step: Merges results and marks batch as complete."""
    
    if isinstance(first_round_results_list, list):  # More robust type check
        first_round_results = {}
        for result in first_round_results_list:
            first_round_results.update(result)
    else:
        first_round_results = first_round_results_list
    
    

    if catch_all_results:
        for email, new_status in catch_all_results.items():
                if email in first_round_results:  #  Only update existing keys
                    first_round_results[email] = new_status 

    batch_tasks[batch_id]["status"] = "completed"
    # Ensure 'results' key is set
    if "results" not in batch_tasks[batch_id]:
        batch_tasks[batch_id]["results"] = first_round_results
        print("Final update triggered for:", batch_id)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(update_frontend_async(100, batch_id))
    loop.close()

    return {"batch_id": batch_id, "results": first_round_results}

@shared_task
def verify_emails_in_parallel(sender, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type, wpuser_id, post_id, output_file_name):
    batch_id = f"batch_{int(time.time())}"  # Unique batch ID
    total_emails = len(email_list)
    random_triggers = generate_random_triggers()

    sorted_triggers = sorted(random_triggers)
    quarter_size = len(sorted_triggers) // 3

    first_quarter = sorted_triggers[:quarter_size]
    second_quarter = sorted_triggers[quarter_size:2 * quarter_size]
    third_quarter = sorted_triggers[2 * quarter_size:]

    # Step 2: Divide each quarter into 4 equal parts
    def split_into_parts(lst, num_parts=4):
        """Splits a list into num_parts roughly equal sublists."""
        k, m = divmod(len(lst), num_parts)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_parts)]

    first_quarter_parts = split_into_parts(first_quarter)
    second_quarter_parts = split_into_parts(second_quarter)
    third_quarter_parts = split_into_parts(third_quarter)
    print(first_quarter)
    print(first_quarter_parts)
    # Initialize batch tracking
    batch_tasks[batch_id] = {"total": total_emails, "results" : {} , "output_file_name": output_file_name, "initial_count":total_emails,   "completed": 0, "status": "processing", "part1": first_quarter_parts[0], "part2": first_quarter_parts[1], 
                             "part3": first_quarter_parts[2], "part4": first_quarter_parts[3], "post_id" : post_id, "wpuser_id" : wpuser_id, "service_type": service_type }
    print(email_list)
    verification_tasks = group(
        verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id= batch_id, total_emails=total_emails) for email in email_list
    )


    chord(verification_tasks)(process_first_round_results.s(batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, second_quarter_parts, third_quarter_parts))

    return


# Get progress for a batch
def get_batch_progress(batch_id):
    if batch_id not in batch_tasks:
        return {"error": "Batch ID not found"}

    completed = batch_tasks[batch_id]["completed"]
    total = batch_tasks[batch_id]["total"]
    progress = int((completed / total) * 100) if total > 0 else 0

    return {"batch_id": batch_id, "progress": progress, "status": batch_tasks[batch_id]["status"]}

# Monitor and clean up completed batches
def manage_email_batches():
    while True:
        for batch_id in list(batch_tasks.keys()):
            if batch_tasks[batch_id]["completed"] == batch_tasks[batch_id]["total"]:
                batch_tasks[batch_id]["status"] = "completed"
                del batch_tasks[batch_id] # Remove completed batch
        global dns_cache, no_mx_domains
        # Clear cache after 1 hr
        dns_cache = {}
        no_mx_domains = set()
        time.sleep(3600)  # Check every 10 seconds

# Run the batch manager in a separate thread
batch_manager_thread = threading.Thread(target=manage_email_batches, daemon=True)
batch_manager_thread.start()





