"""
Google Drive to HubSpot Integration
---------------------------------

This script automates the process of transferring files from Google Drive to HubSpot contacts.
It matches files with contacts based on sharing permissions and creates engagements in HubSpot.

Features:
- Google Drive OAuth2 authentication
- HubSpot Private App token authentication
- Automatic file format conversion
- Asynchronous file processing
- Error handling and logging

Dependencies:
- google-auth-oauthlib
- google-api-python-client
- hubspot-api-client
- aiohttp
- python-dotenv

Environment Variables:
- GOOGLE_CREDENTIALS_PATH: Path to Google OAuth credentials file
- HUBSPOT_ACCESS_TOKEN: HubSpot Private App access token
- GOOGLE_FOLDER_ID: ID of the Google Drive folder to process

Author: Saranraj Santhanam
Date: 11/11/2024
Version: 1.0.0
"""




"""
    Main execution function for the Google Drive to HubSpot integration.
    
    Workflow:
    1. Initialize Google Drive and HubSpot services
    2. Retrieve files from specified Google Drive folder
    3. Process each file asynchronously:
        - Download file content
        - Get sharing permissions
        - Find matching HubSpot contacts
        - Upload file to HubSpot
        - Create engagement with contact
    4. Handle errors and provide feedback
    
    Environment Variables Required:
    - GOOGLE_CREDENTIALS_PATH
    - HUBSPOT_ACCESS_TOKEN
    - GOOGLE_FOLDER_ID
    
    Raises:
        ValueError: If required environment variables are missing
        Exception: For any other unexpected errors
    """


import os
import asyncio
import time
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import pickle
from dotenv import load_dotenv
from hubspot import HubSpot
import requests
import json

# Load environment variables
# Initialize environment variables from .env file

load_dotenv()

# Constants
# Define supported Google Workspace file types and their export formats
# This ensures documents, spreadsheets, and presentations are converted to PDF

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
EXPORT_FORMATS = {
    'application/vnd.google-apps.document': 'application/pdf',
    'application/vnd.google-apps.spreadsheet': 'application/pdf',
    'application/vnd.google-apps.presentation': 'application/pdf'
}

def get_google_drive_service():
    """Set up Google Drive API service"""
    """
    Initialize and return Google Drive API service
    Handles OAuth2 authentication flow:
    1. Checks for existing credentials in token.pickle
    2. Refreshes expired credentials
    3. Initiates new OAuth flow if needed
    4. Saves credentials for future use
    """

    creds = None
    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
    
    if not credentials_path:
        raise ValueError("GOOGLE_CREDENTIALS_PATH not set in environment variables")
        
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
            
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def get_file_content(drive_service, file_id, mime_type):
    """Get file content with proper handling of Google Workspace files"""
    """ Download file content from Google Drive
    Handles different file types:
    1. Google Workspace files (Docs, Sheets, Slides) - exports to PDF
    2. Regular files - downloads directly
    Returns file content as bytes
    """
    try:
        if mime_type in EXPORT_FORMATS:
            request = drive_service.files().export_media(
                fileId=file_id,
                mimeType=EXPORT_FORMATS[mime_type]
            )
        else:
            request = drive_service.files().get_media(fileId=file_id)
            
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
        return file.getvalue()
    except Exception as e:
        print(f"Error downloading file {file_id}: {str(e)}")
        return None

def get_hubspot_client():
    """Set up HubSpot client using Private App token"""
    """
    Initialize HubSpot client using Private App token
    Validates token presence in environment variables
    Returns authenticated HubSpot client instance
    """
    access_token = os.getenv('HUBSPOT_ACCESS_TOKEN')
    if not access_token:
        raise ValueError("HUBSPOT_ACCESS_TOKEN not set in environment variables")
    
    return HubSpot(access_token=access_token)

async def upload_file_to_hubspot(hubspot_client, file_name, file_content, mime_type, contact_id):
    """Upload file to HubSpot and associate it with contact"""
    """
    Upload file to HubSpot and create engagement
    Process:
    1. Upload file to HubSpot File Manager
    2. Create engagement (note) with file attachment
    3. Associate engagement with contact
    
    Parameters:
    - hubspot_client: Authenticated HubSpot client
    - file_name: Name of the file to upload
    - file_content: Binary content of the file
    - mime_type: File's MIME type
    - contact_id: HubSpot contact ID to associate with
    """
    try:
        url = 'https://api.hubapi.com/filemanager/api/v3/files/upload'
        headers = {
            'Authorization': f'Bearer {os.getenv("HUBSPOT_ACCESS_TOKEN")}'
        }
        
        # Create files dictionary with all required fields
        files = {
            'fileName': (None, file_name),
            'file': (
                file_name, 
                file_content,
                'application/pdf' if mime_type in EXPORT_FORMATS else mime_type
            ),
            'folderPath': (None, '/imported-files'),  # Specify the folder path
            'options': (
                None,
                json.dumps({
                    "access": "PRIVATE",
                    "duplicateValidationStrategy": "NONE",
                    "duplicateValidationScope": "EXACT_FOLDER"
                }),
                'application/json'
            )
        }
        
        # Make the request
        upload_response = requests.post(url, headers=headers, files=files)
        
        if upload_response.status_code != 200:
            print(f"Failed to upload file: {upload_response.status_code}")
            print(f"Response: {upload_response.text}")
            return False
            
        file_id = upload_response.json()['objects'][0]['id']
        
        engagement_url = 'https://api.hubapi.com/engagements/v1/engagements'
        engagement_headers = {
            'Authorization': f'Bearer {os.getenv("HUBSPOT_ACCESS_TOKEN")}',
            'Content-Type': 'application/json'
        }
        
        engagement_data = {
            "engagement": {
                "active": True,
                "type": "NOTE",
                "timestamp": int(time.time() * 1000)
            },
            "associations": {
                "contactIds": [contact_id]
            },
            "attachments": [{"id": file_id}],
            "metadata": {
                "body": f"File uploaded from Google Drive: {file_name}"
            }
        }
        
        engagement_response = requests.post(
            engagement_url, 
            headers=engagement_headers,
            json=engagement_data
        )
        
        if engagement_response.status_code != 200:
            print(f"Failed to create engagement: {engagement_response.status_code}")
            print(f"Response: {engagement_response.text}")
            return False
            
        return True
        
    except Exception as e:
        print(f"Failed to upload/associate file: {str(e)}")
        return False

async def process_file(drive_service, hubspot_client, file):
    """Process a single file and attach to HubSpot contacts"""
    """
    Process individual Google Drive file
    Workflow:
    1. Download file content
    2. Get file sharing permissions
    3. Extract shared email addresses
    4. Find matching HubSpot contacts
    5. Upload file and create engagements
    
    Parameters:
    - drive_service: Google Drive API service
    - hubspot_client: HubSpot client
    - file: Google Drive file metadata
    """
    try:
        file_content = get_file_content(drive_service, file['id'], file['mimeType'])
        if not file_content:
            print(f"Failed to get content for file: {file['name']}")
            return False

        permissions = drive_service.permissions().list(
            fileId=file['id'],
            fields="permissions(emailAddress)"
        ).execute()
        
        emails = [p.get('emailAddress') for p in permissions.get('permissions', [])
                 if p.get('emailAddress')]
        
        if not emails:
            print(f"No shared emails found for file: {file['name']}")
            return False

        for email in emails:
            try:
                filter_dict = {
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": email
                        }]
                    }]
                }
                
                contact_result = hubspot_client.crm.contacts.search_api.do_search(
                    public_object_search_request=filter_dict
                )
                
                if not contact_result.results:
                    print(f"No HubSpot contact found for email: {email}")
                    continue

                contact_id = contact_result.results[0].id
                
                success = await upload_file_to_hubspot(
                    hubspot_client,
                    file['name'],
                    file_content,
                    file['mimeType'],
                    contact_id
                )
                
                if success:
                    print(f"Successfully attached {file['name']} to contact {email}")
                
            except Exception as e:
                print(f"HubSpot API error for {email}: {str(e)}")
                continue
            except Exception as e:
                print(f"Error processing email {email} for file {file['name']}: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        print(f"Error processing file {file['name']}: {str(e)}")
        return False

async def main():
    """
    Main application entry point
    Process:
    1. Initialize Google Drive and HubSpot services
    2. Get files from specified Google Drive folder
    3. Process files concurrently using asyncio
    4. Handle errors and exceptions
    
    Environment Variables Required:
    - GOOGLE_CREDENTIALS_PATH: Path to Google OAuth credentials
    - HUBSPOT_ACCESS_TOKEN: HubSpot Private App token
    - GOOGLE_FOLDER_ID: ID of Google Drive folder to process
    """
    try:
        drive_service = get_google_drive_service()
        hubspot_client = get_hubspot_client()
        
        folder_id = os.getenv('GOOGLE_FOLDER_ID')
        if not folder_id:
            raise ValueError("GOOGLE_FOLDER_ID not set in environment variables")
            
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(id, name, mimeType)"
        ).execute()
        
        files = results.get('files', [])
        print(f"Found {len(files)} files in Google Drive folder")
        
        tasks = [process_file(drive_service, hubspot_client, file) for file in files]
        await asyncio.gather(*tasks)
        
    except Exception as e:
        print(f"Application failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())