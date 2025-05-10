import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import logging
from io import StringIO
from datetime import datetime


"""
This script pulls stats and performance data from the English Premier League page on fbref.com 
and neatly saves everything into a Google Sheets file using the Google Sheets API.
"""

# Configuration and Setup 
SPREADSHEET_KEY = "key"
print(f" Spreadsheet Key Acquired: {SPREADSHEET_KEY}")      #To confirm key is loaded correctly

# Log file setup
logging.basicConfig(filename='run_log.txt', level=logging.INFO,
                    format='[%(asctime)s] - %(levelname)s - %(message)s')

# Connecting to Google Sheets
try:
    auth_scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_file("credentials.json", scopes=auth_scopes)     #
    gs_client = gspread.authorize(credentials)
    spreadsheet = gs_client.open_by_key(SPREADSHEET_KEY)
    spreadsheet.update_title("EPL Insights - 2024 Season")
except Exception as error:
    logging.error(f"Google Sheets connection error: {error}")
    raise RuntimeError("Unable to connect to Google Sheets. Please verify your credentials.")

# Clear old worksheets except the first one since google sheet needs at least one sheet
tabs = spreadsheet.worksheets()
for idx, tab in enumerate(tabs):
    if idx > 0:
        spreadsheet.del_worksheet(tab)
tabs[0].clear()

# Scraping the premier league site
stats_url = "https://fbref.com/en/comps/9/Premier-League-Stats"

try:
    html_data = urlopen(stats_url).read().decode("utf-8")
    page_soup = BeautifulSoup(html_data, "lxml")
    all_tables = page_soup.find_all("table")
except Exception as error:
    logging.error(f"Web data retrieval issue: {error}")
    raise RuntimeError("Failed to retrieve EPL data. Verify the source website is live.")

# Exporting tables to our google sheet
for tbl in all_tables:
    try:
        # Extract table title and ID
        tbl_title = tbl.find("caption").get_text(strip=True)
        tbl_id = tbl.get("id", "NoID")
        tab_name = f"{tbl_title}_{tbl_id}".strip()

        # Converting HTML table to DataFrame
        data_frame = pd.read_html(StringIO(str(tbl)))[0]

        # Flattening multi-index headers if necessary
        if isinstance(data_frame.columns, pd.MultiIndex):
            data_frame.columns = [' '.join(col).strip() for col in data_frame.columns]

        # Check if sheet exists or create a new one
        try:
            current_tab = spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            current_tab = spreadsheet.add_worksheet(title=tab_name, rows="200", cols="26")
        else:
            current_tab.clear()

        # Append processing timestamp
        data_frame["Retrieved On"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Push to sheet
        set_with_dataframe(current_tab, data_frame)
        logging.info(f"Data for '{tab_name}' loaded successfully.")

    except Exception as error:
        logging.warning(f"Skipping table '{tbl_id}' due to error: {error}")
        continue


sheet_link = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_KEY}"
print(f"\n EPL stats have been loaded into Google Sheets ")
print(f"Access your sheet here: {sheet_link}")
