import os
import re
import base64
import json
import ollama
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime
from zoneinfo import ZoneInfo
from googleapiclient.discovery import build
import traceback
import time

# Configuration
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

SUBJECT_FILTER = "[SCC - SPN]"  # Simplified to single filter
SPREADSHEET_ID = '1_i7VbqufcwaKwirgbW3CMyyregITsX2Fxl2M8MMEQVE'
SHEET_NAME = 'Sheet 1'
OLLAMA_MODEL = 'gemma3:4b'
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

# Option to mark emails as read after processing (set to False to leave them unread)
MARK_AS_READ_AFTER_PROCESSING = True

def get_google_credentials():
    """Get Google API credentials using OAuth 2.0"""
    creds = None
    
    # Check if we need to delete the token file due to scope changes
    if os.path.exists(TOKEN_FILE):
        try:
            # Try to load existing credentials
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            
            # Check if the scopes match what we need
            if hasattr(creds, 'scopes') and creds.scopes:
                missing_scopes = set(SCOPES) - set(creds.scopes)
                if missing_scopes:
                    print(f"Token has missing scopes: {missing_scopes}")
                    print("Deleting token.json to re-authenticate with correct scopes...")
                    os.remove(TOKEN_FILE)
                    creds = None
        except Exception as e:
            print(f"Error loading existing token: {e}")
            print("Deleting token.json and re-authenticating...")
            os.remove(TOKEN_FILE)
            creds = None
    
    # If credentials are invalid or expired, refresh or get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Token refresh failed: {e}")
                print("Getting new credentials...")
                creds = None
        
        if not creds:
            if not os.path.exists(CREDENTIALS_FILE):
                raise Exception(f"Credentials file '{CREDENTIALS_FILE}' not found. Please download it from Google Cloud Console.")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    return creds

def extract_email_date(message):
    """Extract the sent date from email headers"""
    try:
        headers = message.get('payload', {}).get('headers', [])
        for header in headers:
            if header['name'].lower() == 'date':
                # Parse the date string
                date_str = header['value']
                # Gmail date format is usually like: "Wed, 25 Dec 2024 10:30:00 +0000"
                try:
                    # Try to parse common email date formats
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(date_str)
                    # Convert to local timezone or keep as UTC
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"    Warning: Could not parse date '{date_str}': {e}")
                    return date_str  # Return raw date string if parsing fails
        return ""  # Return empty if no date header found
    except Exception as e:
        print(f"    Error extracting email date: {e}")
        return ""

def extract_email_content(message):
    """Recursively find plain text body in email parts"""
    def find_text(part):
        # Check if this part is plain text
        if part['mimeType'] == 'text/plain':
            if 'data' in part['body']:
                data = part['body']['data']
                return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
        
        # Check nested parts
        if 'parts' in part:
            for subpart in part['parts']:
                text = find_text(subpart)
                if text:
                    return text
        return ""

    payload = message.get('payload', {})
    return find_text(payload)

def process_email_with_ollama(email_content):
    response = ollama.generate(
        model=OLLAMA_MODEL,
        prompt=f"""
        Analyze this resource fair email and extract:
        1. Summary of main content (50 words max)
        2. Contact information (names, emails, phones) - extract ALL contacts mentioned
        3. Determine if this is requesting assistance or providing assistance
        
        Return ONLY in this JSON format:
        {{
            "summary": "concise summary",
            "contacts": [
                {{
                    "name": "full name or organization",
                    "email": "email address",
                    "phone": "phone number"
                }}
            ],
            "assistance_type": "requesting" or "providing"
        }}
        
        Email Content:
        {email_content[:10000]}  # Truncate to avoid context limits
        """
    )
    return response['response']

def parse_ollama_response(ollama_output):
    try:
        print(f"  Debug - Raw Ollama output: {ollama_output[:200]}...")  # Show first 200 chars
        
        # Extract JSON from Ollama response
        json_match = re.search(r'\{.*\}', ollama_output, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            parsed = json.loads(json_str)
            
            # Validate and clean the parsed data
            result = {
                "summary": parsed.get('summary', 'No summary provided').strip(),
                "contacts": [],
                "assistance_type": parsed.get('assistance_type', 'unknown').strip().lower()
            }
            
            # Clean and validate contacts
            raw_contacts = parsed.get('contacts', [])
            if isinstance(raw_contacts, list):
                for contact in raw_contacts:
                    if isinstance(contact, dict):
                        clean_contact = {
                            'name': str(contact.get('name', '')).strip() if contact.get('name') else '',
                            'email': str(contact.get('email', '')).strip() if contact.get('email') else '',
                            'phone': str(contact.get('phone', '')).strip() if contact.get('phone') else ''
                        }
                        # Only add contact if it has at least one non-empty field
                        if any([clean_contact['name'], clean_contact['email'], clean_contact['phone']]):
                            result['contacts'].append(clean_contact)
            
            # Ensure assistance_type is standardized
            if result["assistance_type"] not in ['requesting', 'providing']:
                if any(word in result["assistance_type"] for word in ['request', 'need', 'looking', 'seeking']):
                    result["assistance_type"] = 'requesting'
                elif any(word in result["assistance_type"] for word in ['provid', 'offer', 'available', 'help']):
                    result["assistance_type"] = 'providing'
                else:
                    result["assistance_type"] = 'unknown'
            
            print(f"  Debug - Parsed summary: '{result['summary']}'")
            print(f"  Debug - Parsed assistance_type: '{result['assistance_type']}'")
            print(f"  Debug - Parsed contacts: {len(result['contacts'])} found")
            
            return result
            
    except json.JSONDecodeError as e:
        print(f"  Error - JSON parsing failed: {e}")
        print(f"  Error - Ollama output was: {ollama_output}")
    except Exception as e:
        print(f"  Error - General parsing error: {e}")
    
    # Fallback response
    return {
        "summary": f"Parsing failed - Raw output: {ollama_output[:100]}",
        "contacts": [],
        "assistance_type": "unknown"
    }

def get_sheet_service(creds):
    """Create Google Sheets service"""
    return build('sheets', 'v4', credentials=creds)

def get_gmail_service(creds):
    """Create Gmail service"""
    return build('gmail', 'v1', credentials=creds)

def setup_sheet_headers(service):
    """Set up headers in the Google Sheet if they don't exist"""
    try:
        # Check if headers already exist
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:G1"
        ).execute()
        
        existing_values = result.get('values', [])
        
        # Expected headers
        headers = ['Name', 'Email', 'Phone', 'Summary', 'Assistance Type', 'Sent Date', 'Email Link']
        
        # If no headers or incomplete headers, set them up
        if not existing_values or len(existing_values[0]) < 7:
            print("Setting up sheet headers...")
            header_body = {
                'values': [headers]
            }
            
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!A1:G1",
                valueInputOption='USER_ENTERED',
                body=header_body
            ).execute()
            
            print("  ✓ Headers set up successfully")
        else:
            print("  Headers already exist")
            
    except Exception as e:
        print(f"  Warning: Could not set up headers: {str(e)}")
        # Continue anyway - headers are not critical

def test_sheet_access(service):
    """Test if we have write access to the Google Sheet"""
    try:
        # First, let's see what sheets exist in this spreadsheet
        print("Checking available sheets...")
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = spreadsheet.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        print(f"  Available sheets: {sheet_names}")
        
        if SHEET_NAME not in sheet_names:
            print(f"  ERROR: Sheet '{SHEET_NAME}' not found!")
            print(f"  Available sheets: {sheet_names}")
            print(f"  Please update SHEET_NAME in your code to match one of the available sheets")
            return False
        
        # Try to read the sheet first
        print("Testing sheet read access...")
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1:G1"  # Updated to G for 7 columns
        ).execute()
        print("  Read access: OK")
        
        # Try to write a test row
        print("Testing sheet write access...")
        test_data = {
            'values': [['TEST', 'TEST', 'TEST', 'TEST', 'TEST', 'TEST', 'TEST']]  # 7 columns
        }
        
        # Find the next available row
        all_data = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:A"
        ).execute()
        existing_rows = all_data.get('values', [])
        test_row = len(existing_rows) + 1
        
        # Try to append the test data
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A{test_row}:G{test_row}",  # Updated to G for 7 columns
            valueInputOption='USER_ENTERED',
            body=test_data
        ).execute()
        
        # Clean up the test data
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A{test_row}:G{test_row}"  # Updated to G for 7 columns
        ).execute()
        
        print("  Write access: OK")
        return True
        
    except Exception as e:
        print(f"  Sheet access test failed: {str(e)}")
        if "403" in str(e) or "permission" in str(e).lower():
            print("  Issue: No write permission to the sheet")
            print("  Solutions to try:")
            print("    1. Delete token.json and re-authenticate")
            print("    2. Check OAuth scopes in Google Cloud Console")
            print("    3. Ensure both Gmail API and Sheets API are enabled")
        elif "404" in str(e) or "not found" in str(e).lower():
            print("  Issue: Sheet not found or wrong SPREADSHEET_ID")
        return False

def get_all_unread_messages(gmail_service, query):
    """Get all unread messages matching the query using pagination"""
    all_messages = []
    next_page_token = None
    page_count = 0
    
    while True:
        page_count += 1
        print(f"  Fetching page {page_count} of unread email results...")
        
        try:
            # Make the API call with pagination
            if next_page_token:
                results = gmail_service.users().messages().list(
                    userId='me', 
                    q=query, 
                    pageToken=next_page_token,
                    maxResults=500  # Maximum allowed per page
                ).execute()
            else:
                results = gmail_service.users().messages().list(
                    userId='me', 
                    q=query,
                    maxResults=500  # Maximum allowed per page
                ).execute()
            
            messages = results.get('messages', [])
            all_messages.extend(messages)
            print(f"    Found {len(messages)} unread emails on this page (total so far: {len(all_messages)})")
            
            # Check if there are more pages
            next_page_token = results.get('nextPageToken')
            if not next_page_token:
                break
                
            # Small delay to be respectful to the API
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching page {page_count}: {str(e)}")
            break
    
    print(f"Total unread emails found across all pages: {len(all_messages)}")
    return all_messages

def mark_email_as_read(gmail_service, message_id):
    """Mark an email as read by removing the UNREAD label"""
    try:
        gmail_service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        return True
    except Exception as e:
        print(f"  Warning: Could not mark email as read: {str(e)}")
        return False

def format_phone_number(phone):
    """Format phone number to (123) 456-7890 format"""
    if not phone:
        return ''
    
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, str(phone)))
    
    # Handle different lengths
    if len(digits) == 10:
        # Format as (123) 456-7890
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith('1'):
        # Remove leading 1 and format
        digits = digits[1:]
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 7:
        # Format as 123-4567 (local number)
        return f"{digits[:3]}-{digits[3:]}"
    else:
        # Return original if can't format properly, but ensure it's safe for sheets
        safe_phone = str(phone).strip()
        # Prevent formula injection by adding apostrophe if it starts with =, +, -, @
        if safe_phone and safe_phone[0] in ['=', '+', '-', '@']:
            safe_phone = "'" + safe_phone
        return safe_phone

def append_single_email_to_sheet(service, email_data):
    """Append a single email's data to Google Sheet immediately as table rows"""
    try:
        # Prepare the data with format: Name, Email, Phone, Summary, Request/Provide Assistance, Sent Day, Email Link
        values = []
        contacts = email_data.get('contacts', [])
        summary = email_data.get('summary', 'No summary available')
        assistance_type = email_data.get('assistance_type', 'unknown')
        sent_day = email_data.get('sent_day', '')
        email_link = email_data['email_link']
        
        # Ensure summary is safe for sheets (prevent formula injection)
        if summary and summary[0] in ['=', '+', '-', '@']:
            summary = "'" + summary
        
        # Debug: Print what we're about to add
        print(f"  Debug - Summary: '{summary[:50]}...'")
        print(f"  Debug - Assistance type: '{assistance_type}'")
        print(f"  Debug - Contacts found: {len(contacts)}")
        
        # If there are multiple contacts, create a row for each contact
        if contacts:
            for i, contact in enumerate(contacts):
                # Safe extraction with null checks
                contact_name = (contact.get('name') or '').strip() if contact.get('name') else ''
                contact_email = (contact.get('email') or '').strip() if contact.get('email') else ''
                raw_phone = (contact.get('phone') or '').strip() if contact.get('phone') else ''
                
                # Format phone number properly
                contact_phone = format_phone_number(raw_phone)
                
                # Prevent formula injection for name and email too
                if contact_name and contact_name[0] in ['=', '+', '-', '@']:
                    contact_name = "'" + contact_name
                if contact_email and contact_email[0] in ['=', '+', '-', '@']:
                    contact_email = "'" + contact_email
                
                print(f"  Debug - Contact {i+1}: Name='{contact_name}', Email='{contact_email}', Phone='{contact_phone}' (original: '{raw_phone}')")
                
                values.append([
                    contact_name,      # Column A: Name
                    contact_email,     # Column B: Email
                    contact_phone,     # Column C: Phone (formatted)
                    summary,           # Column D: Summary
                    assistance_type,   # Column E: Request/Provide Assistance
                    sent_day,          # Column F: Sent Day
                    email_link         # Column G: Email Link
                ])
        else:
            # If no contacts, still add a row with the summary and assistance type
            print("  Debug - No contacts found, adding row with empty contact fields")
            values.append([
                '',                # Column A: Name (empty)
                '',                # Column B: Email (empty)
                '',                # Column C: Phone (empty)
                summary,           # Column D: Summary
                assistance_type,   # Column E: Request/Provide Assistance
                sent_day,          # Column F: Sent Day
                email_link         # Column G: Email Link
            ])
        
        body = {
            'values': values
        }
        
        # Debug: Print the exact data being sent
        print(f"  Debug - Rows being sent to sheet: {len(values)}")
        for i, row in enumerate(values):
            print(f"    Row {i+1}: [Name='{row[0]}', Email='{row[1]}', Phone='{row[2]}', Summary='{row[3][:30]}...', Type='{row[4]}']")
        
        # Use append to add to the end of the sheet/table
        request = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:G",
            valueInputOption='USER_ENTERED',  # This interprets the data properly
            insertDataOption='INSERT_ROWS',   # This adds new rows to the table
            body=body
        )
        response = request.execute()
        
        rows_added = len(values)
        print(f"  ✓ Added {rows_added} row(s) to Google Sheet table")
        return True
        
    except Exception as e:
        print(f"  ✗ Error adding to Google Sheet: {str(e)}")
        print(f"  ✗ Exception details: {type(e).__name__}: {str(e)}")
        import traceback
        print(f"  ✗ Traceback: {traceback.format_exc()}")
        return False

def main():
    print("Starting email processing for ALL unread [SCC-SPN] emails...")
    print("Each email will be added to the Google Sheet immediately after processing.")
    
    # Get Google credentials
    print("Authenticating with Google...")
    try:
        creds = get_google_credentials()
    except Exception as e:
        print(f"Authentication failed: {str(e)}")
        return
    
    # Set up services
    print("Setting up Google Services...")
    try:
        sheet_service = get_sheet_service(creds)
        gmail_service = get_gmail_service(creds)
        
        # Debug: Show which account we're authenticated as
        profile = gmail_service.users().getProfile(userId='me').execute()
        authenticated_email = profile.get('emailAddress')
        print(f"Authenticated as: {authenticated_email}")
        
        # Test sheet access before processing emails
        print(f"\nTesting access to Google Sheet...")
        print(f"Sheet ID: {SPREADSHEET_ID}")
        print(f"Sheet name: {SHEET_NAME}")
        
        if not test_sheet_access(sheet_service):
            print("\nERROR: Cannot access the Google Sheet!")
            print("Please fix the sheet permissions before continuing.")
            print(f"Share the sheet with: {authenticated_email} (as Editor)")
            return
        
        print("Sheet access verified! Setting up headers...\n")
        
        # Set up sheet headers
        setup_sheet_headers(sheet_service)
        
        print("Proceeding with email processing...\n")
        
    except Exception as e:
        print(f"Service setup failed: {str(e)}")
        return
    
    # Search for unread emails with the SCC-SPN subject filter
    query = f'subject:"{SUBJECT_FILTER}" is:unread'
    print(f"Searching unread emails with query: {query}")
    
    try:
        messages = get_all_unread_messages(gmail_service, query)
        print(f"Total unread emails to process: {len(messages)}")
    except Exception as e:
        print(f"Gmail search error: {str(e)}")
        return
    
    if not messages:
        print("No unread emails found matching the criteria")
        return
    
    # Counters for summary
    successful_processed = 0
    failed_processed = 0
    emails_marked_read = 0
    sheet_updates_successful = 0
    sheet_updates_failed = 0
    
    for i, message in enumerate(messages):
        try:
            print(f"\nProcessing email {i+1}/{len(messages)} (Success: {successful_processed}, Failed: {failed_processed})")
            msg = gmail_service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            
            # Check if email is still unread (in case it was read between search and processing)
            labels = msg.get('labelIds', [])
            if 'UNREAD' not in labels:
                print(f"  Email is no longer unread, skipping")
                continue
            
            # Extract content and date
            email_content = extract_email_content(msg)
            if not email_content:
                print(f"  No text content found, skipping")
                failed_processed += 1
                continue
            
            # Extract sent date
            sent_day = extract_email_date(msg)
            print(f"  Sent date: {sent_day}")
                
            email_link = f"https://mail.google.com/mail/u/0/#inbox/{message['id']}"
            print(f"  Email link: {email_link}")
            print(f"  Content length: {len(email_content)} characters")
            
            # Process with Ollama
            print("  Processing with Ollama...")
            ollama_response = process_email_with_ollama(email_content)
            parsed_data = parse_ollama_response(ollama_response)
            print(f"  Summary: {parsed_data.get('summary', '')[:100]}...")
            
            # Prepare data for Google Sheet
            email_data = {
                'email_link': email_link,
                'summary': parsed_data.get('summary', ''),
                'contacts': parsed_data.get('contacts', []),
                'assistance_type': parsed_data.get('assistance_type', 'unknown'),
                'sent_day': sent_day
            }
            
            # Add to Google Sheet immediately
            print("  Adding to Google Sheet...")
            if append_single_email_to_sheet(sheet_service, email_data):
                sheet_updates_successful += 1
                successful_processed += 1
                print("  ✓ Email processed and added to sheet successfully")
                
                # Mark email as read ONLY after successful sheet update
                if MARK_AS_READ_AFTER_PROCESSING:
                    if mark_email_as_read(gmail_service, message['id']):
                        emails_marked_read += 1
                        print("  ✓ Marked as read")
                    
            else:
                sheet_updates_failed += 1
                print("  ✗ Failed to add to sheet - email NOT marked as read")
                # Don't mark as read if sheet update failed
            
            # Add a small delay between email processing to be respectful to APIs
            time.sleep(0.5)  # Slightly longer delay for individual updates
            
        except Exception as e:
            print(f"  ✗ Error processing email: {str(e)}")
            print(traceback.format_exc())
            failed_processed += 1
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETE - SUMMARY")
    print(f"{'='*60}")
    print(f"Total emails found: {len(messages)}")
    print(f"Successfully processed: {successful_processed}")
    print(f"Failed to process: {failed_processed}")
    print(f"Sheet updates successful: {sheet_updates_successful}")
    print(f"Sheet updates failed: {sheet_updates_failed}")
    if MARK_AS_READ_AFTER_PROCESSING:
        print(f"Emails marked as read: {emails_marked_read}")
    else:
        print("Emails were NOT marked as read (MARK_AS_READ_AFTER_PROCESSING = False)")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()