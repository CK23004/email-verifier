from celery import shared_task
import socks
import smtplib
import socket
import logging
import asyncio
import aiodns, sys
import time  # For tracking time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


@shared_task
def process_email_list(email_list, post_id, user_id, output_file_name, email_count):
    try:
        
        
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        # Set up logging to capture socket-level communication
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

        # Custom SMTP class to use SOCKS5 proxy
        class ProxySMTP(smtplib.SMTP):
            def __init__(self, host='', port=0, proxy_host=None, proxy_port=None, proxy_user=None, proxy_password=None, *args, **kwargs):
                self.proxy_host = proxy_host
                self.proxy_port = proxy_port
                self.proxy_user = proxy_user
                self.proxy_password = proxy_password
                super().__init__(host, port, *args, **kwargs)

            def _get_socket(self, host, port, timeout):
                # Create a socket that will connect through the SOCKS5 proxy
                sock = socks.socksocket()
                sock.set_proxy(socks.SOCKS5, self.proxy_host, self.proxy_port, username=self.proxy_user, password=self.proxy_password)
                sock.settimeout(timeout)  # You can increase this timeout
                sock.connect((host, port))  # Connect to the target SMTP server
                return sock
            
        dns_cache = {}
        # Function to fetch MX records asynchronously using aiodns
        async def fetch_mx_records_async(domain):
            """
            Fetch MX records asynchronously using aiodns.
            """
            if domain in dns_cache:
                logging.info(f"Using cached MX records for domain: {domain}")
                return dns_cache[domain]
            timeout = 10
            resolver = aiodns.DNSResolver(timeout=timeout)  # Default system nameserver
            try:
                logging.info(f"Fetching MX records for domain: {domain}")
                start_time = time.time()  # Start time to measure MX record fetch duration
                answers = await resolver.query(domain, 'MX')
                mx_records = sorted([(record.priority, record.host) for record in answers], key=lambda x: x[0])
                end_time = time.time()  # End time after fetching MX records
                logging.info(f"MX records found: {mx_records}")
                logging.debug(f"Time taken to fetch MX records: {end_time - start_time:.2f} seconds")
                dns_cache[domain] = [record[1] for record in mx_records]
                return dns_cache[domain]
            except aiodns.error.DNSError as e:
                logging.error(f"DNS error while fetching MX records for {domain}: {e}")
                return []
            except Exception as e:
                logging.error(f"Unexpected error while fetching MX records: {e}")
                return []


        # Wrapper to integrate asyncio calls in ThreadPoolExecutor
        def fetch_mx_records(domain):
            """
            Wrapper to run the async fetch_mx_records_async in a blocking way.
            """
            return asyncio.run(fetch_mx_records_async(domain))

        no_mx_domains = set()
        # Function to verify an email address via SMTP using a proxy
        def verify_email_via_smtp(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password):
            domain = recipient.split('@')[1]
            if domain in no_mx_domains:
                return recipient, "No MX Records Found"
            
            mx_records = fetch_mx_records(domain)
            
            if not mx_records:
                no_mx_domains.add(domain)
                return recipient, "No MX Records Found"

            # Use the first MX record for verification
            smtp_server = mx_records[0]
            smtp_port = 25

            try:
                start_time = time.time()  # Start time to measure SMTP connection duration
                logging.debug(f"Connecting to SMTP server {smtp_server} on port {smtp_port} via proxy {proxy_host}:{proxy_port}")
                
                # Use ProxySMTP class for the connection
                with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:  # Increased timeout to 60 seconds
                    response = server.ehlo()
                    logging.debug(f"EHLO Response: {response}")
                    
                    response = server.mail(sender)
                    logging.debug(f"MAIL FROM Response: {response}")
                    
                    code, message = server.rcpt(recipient)
                    server_message = message.decode('utf-8')
                    logging.info(f"Message type: {type(server_message)}")

                    # message = message.decode('utf-8')
                    logging.debug(f"RCPT TO Response: {code} - {message.decode('utf-8')}")
                    
                    end_time = time.time()  # End time after SMTP communication
                    logging.debug(f"Time taken for SMTP verification: {end_time - start_time:.2f} seconds")
                    
                    # Check if the email is valid based on common SMTP success codes
                    if code in [250, 251, 252]:
                        return recipient, "Valid"
                    elif code in [550, 551, 552, 553, 554]:
                        if 'spam policy' in server_message.lower() or 'blocked using spamhaus' in server_message.lower():
                            return recipient, "Spam Block"
                        else:    
                            return recipient, "Invalid"
                    elif code in [450, 451, 452, 421]:
                        return recipient, "Temporarily Limited"
                    else:
                        return recipient, "Unknown"
            except socket.timeout:
                logging.error(f"Connection to {smtp_server} timed out.")
                return recipient, "Unknown"
            except Exception as e:
                logging.error(f"Error verifying email: {e}")
                return recipient, f"Error: {e}"

        catch_all_domains = set()
        #to check catch all
        def verify_email_for_catchall(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password):
            domain = recipient.split('@')[1]
            if domain in catch_all_domains:
                return recipient, "Catch All"
            mx_records = fetch_mx_records(domain)

            # Use the first MX record for verification
            smtp_server = mx_records[0]
            smtp_port = 25
            invalid_email = f"n1onexistent.8@{domain}"
            try:
                start_time = time.time()  # Start time to measure SMTP connection duration
                logging.debug(f"Connecting to SMTP server {smtp_server} on port {smtp_port} via proxy {proxy_host}:{proxy_port}")
                
                # Use ProxySMTP class for the connection
                with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:  # Increased timeout to 60 seconds
                    response = server.ehlo()
                    logging.debug(f"EHLO Response: {response}")
                    
                    response = server.mail(sender)
                    logging.debug(f"MAIL FROM Response: {response}")
                    
                    code, message = server.rcpt(invalid_email)
                    server_message = message.decode('utf-8')
                    logging.info(f"Message type: {type(server_message)}")

                    # message = message.decode('utf-8')
                    logging.debug(f"RCPT TO Response: {code} - {message.decode('utf-8')}")
                    
                    end_time = time.time()  # End time after SMTP communication
                    logging.debug(f"Time taken for SMTP verification: {end_time - start_time:.2f} seconds")
                    
                    # Check if the email is valid based on common SMTP success codes
                    if code in [250, 251, 252]:
                        catch_all_domains.add(domain)
                        return recipient, "Catch All"
                    else:
                        return recipient, "Valid"
            except socket.timeout:
                logging.error(f"Connection to {smtp_server} timed out.")
                return recipient, "Valid"
            except Exception as e:
                logging.error(f"Error verifying email: {e}")
                return recipient, f"Error: at CA {e}"



        # Function to verify multiple emails in parallel
        def verify_emails_in_parallel(sender, proxy_host, proxy_port, proxy_user, proxy_password, email_list, max_workers=5):
            results = {email: None for email in email_list}
            
            # Using the same proxy for all emails (the proxy server rotates IPs automatically)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_email = {
                    executor.submit(
                        verify_email_via_smtp, sender, email, proxy_host, proxy_port, proxy_user, proxy_password
                    ): email for email in email_list
                }
                for future in as_completed(future_to_email):
                    email = future_to_email[future]
                    try:
                        result = future.result()
                        results[email] = result[1]  # Assuming result[1] is the status
                    except Exception as e:
                        results[email] = f"Error: {str(e)}"

            spam_block_emails = [email for email, status in results.items() if status == 'Spam Block']
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_email = {
                    executor.submit(
                        verify_email_via_smtp, sender, email, proxy_host, proxy_port, proxy_user, proxy_password
                    ): email for email in spam_block_emails
                }
                for future in as_completed(future_to_email):
                    email = future_to_email[future]
                    try:
                        result = future.result()
                        results[email] = result[1]  # Assuming result[1] is the status
                    except Exception as e:
                        results[email] = f"Error: {str(e)}"

            valid_emails = [email for email, status in results.items() if status == 'Valid']
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_email = {
                    executor.submit(
                        verify_email_for_catchall, sender, email, proxy_host, proxy_port, proxy_user, proxy_password
                    ): email for email in valid_emails
                }
                for future in as_completed(future_to_email):
                    email = future_to_email[future]
                    try:
                        result = future.result()
                        results[email] = result[1]  # Assuming result[1] is the status
                    except Exception as e:
                        results[email] = f"Error: {str(e)}"
            
            print(results)
            
            return results

        email_order_dict = {}
        def write_results_to_csv(results, output_file):
            """
            Writes email verification results to a CSV file.

            Args:
                results (list of tuples): A list where each tuple contains (recipient, status).
                output_file (str): The path to the output CSV file.
            """
            ordered_results = sorted(results.items(), key=lambda x: email_order_dict.get(x[0], float('inf')))
            

            # Create a DataFrame from the list of tuples
            df = pd.DataFrame(ordered_results, columns=["Email", "Status"])
            
            # Write the DataFrame to a CSV file
            df.to_csv(output_file, index=False, encoding="utf-8")
          


     
       
        
        sender_email = "kamlesh@sphurti.net"
        # Proxy server details (same proxy for all email verifications)
        proxy_host = "gw-open.netnut.net"
        proxy_port = 9595
        proxy_user = "kamlesh007-evsh-any"
        proxy_password = "Kamleshsurana@007"
        
           
        # Verify emails in parallel
        results = verify_emails_in_parallel(sender_email, proxy_host, proxy_port, proxy_user, proxy_password, email_list, max_workers=3)
        
        # Print the results
        # for recipient, status in results:
        #     print(f"Email: {recipient}, Status: {status}")
        output_file = output_file_name

        # Write results to CSV
        write_results_to_csv(results, output_file)
        return {
                'output_file': output_file,
                'post_id': post_id,
                'user_id': user_id,
                'email_count': email_count
            }
        # end_script_time = time.time()  # End time after script execution
        # logging.info(f"Total execution time: {end_script_time - start_script_time:.2f} seconds")

    except Exception as e:
        return {"error": str(e)}
