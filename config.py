import os
import streamlit as st
from pathlib import Path

# Detect Environment
def is_cloud_env():
    # Streamlit Cloud usually has this env var or we check if secrets are loaded
    return os.environ.get('STREAMLIT_RUNTIME_ENV') == 'cloud' or hasattr(st, "secrets")

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR  # Root in this simple setup
DB_FILENAME = "stockinfocus.db"
DB_PATH = DATA_DIR / DB_FILENAME

# Constants
CREDENTIALS_FILE = "dangvu-n8n-a9b0e98a1f79.json"
TARGET_FILE_NAME = "stockinfocus.db" 
DRIVE_FILE_ID = "12p23dXf_h56Eg2UPzzTKaov02qLuq61j" # Found via list_drive_files.py


def get_credentials_info():
    """
    Returns credentials dictionary either from st.secrets (Cloud) 
    or local JSON file (Local).
    """
    try:
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except FileNotFoundError:
        pass # Secrets file not found locally
    except Exception:
        pass # Other streamlit errors

    
    # Fallback to local file
    json_path = BASE_DIR / CREDENTIALS_FILE
    if json_path.exists():
        import json
        with open(json_path, 'r') as f:
            return json.load(f)
            
    return None
