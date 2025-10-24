"""
AUTHOR: RUDOLF
DATE: 24 Oct 2025


"""
import os
import yaml
import json
import requests
import pandas as pd
from pandas import json_normalize
from datetime import datetime
from dotenv import load_dotenv


# -------------------------------------------------------------
# Debug / Dry Run Controls
# -------------------------------------------------------------
DEBUG = True       # Enable verbose logging
DRY_RUN = True     # Disable API calls and use mock data instead
REGEN_MOCKDATA = True

# -------------------------------------------------------------
# Load configuration and environment variables
# -------------------------------------------------------------
def load_env():
    load_dotenv()
    api_key = os.getenv("SPORTS_GAMES_API_KEY")
    return api_key

# -------------------------------------------------------------
# Main
# -------------------------------------------------------------

def main():
    api_key = load_env()
    print(f"API_KEY: {api_key}")

if __name__ == "__main__":
    main()