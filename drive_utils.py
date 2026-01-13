import os
import io
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime

import config

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_drive_service():
    """Authenticates and returns the Drive service."""
    creds_info = config.get_credentials_info()
    if not creds_info:
        raise Exception("Credentials not found. Check config.py or st.secrets.")
        
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=creds)
    return service

def find_file_id_by_name(service, filename):
    """Searches for a file by name and returns its ID."""
    print(f"Searching for '{filename}' on Drive...")
    query = f"name = '{filename}' and trashed = false"
    results = service.files().list(
        q=query, pageSize=10, fields="nextPageToken, files(id, name, modifiedTime, size)"
    ).execute()
    items = results.get('files', [])

    if not items:
        print(f"No file found with name '{filename}'")
        return None
    
    # Return the first match (most relevant)
    file = items[0]
    print(f"Found file: {file['name']} (ID: {file['id']}, Size: {file.get('size')} bytes)")
    return file['id']

def download_file_from_drive(file_id, local_path):
    """Downloads a file from Drive to local path."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    print(f"Downloading file ID {file_id} to {local_path}...")
    while done is False:
        status, done = downloader.next_chunk()
        # print(f"Download {int(status.progress() * 100)}%.")
    
    with open(local_path, 'wb') as f:
        f.write(fh.getbuffer())
    print("Download complete.")
    
def check_and_update_db():
    """
    Main function to sync DB.
    1. Connect to Drive.
    2. Download to local using Configured ID.
    """
    try:
        service = get_drive_service()
        file_id = getattr(config, 'DRIVE_FILE_ID', None)
        
        if not file_id:
            file_id = find_file_id_by_name(service, config.TARGET_FILE_NAME)
        
        if file_id:
            download_file_from_drive(file_id, config.DB_PATH)
            return True, "Update successful"
        else:
            return False, f"File '{config.TARGET_FILE_NAME}' not found on Drive."
            
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    # Test script
    success, msg = check_and_update_db()
    print(msg)
