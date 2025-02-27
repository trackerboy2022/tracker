import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import os
import json

def extract_table_data(url):
    """
    Extracts data from the artist chart history table on the given URL.

    Args:
        url: The URL of the webpage containing the table.

    Returns:
        A list of dictionaries, where each dictionary represents a row in the table.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table container
        table_container = soup.select_one('.artist-chart-history-container .artist-chart-history-items')
        if not table_container:
            print("Table container not found.")
            return []

        # Find all rows in the table
        rows = table_container.select('DIV.o-chart-results-list-row')
        if not rows:
            print("No rows found in the table.")
            return []

        table_data = []
        for row in rows:
            row_data = {}

            # Extract title
            title_element = row.select_one('H3#title-of-a-story.c-title')
            row_data['title'] = title_element.text.strip() if title_element else ''

            # Extract artist
            artist_element = row.select_one('SPAN.c-label')
            row_data['artist'] = artist_element.text.strip() if artist_element else ''

            # Extract additional data cells
            additional_data_container = row.select_one('DIV.lrv-u-flex.lrv-u-height-100p.u-background-color-grey-lightest\\@mobile-max.u-height-37\\@mobile-max')
            if additional_data_container:
                data_cells = additional_data_container.select('DIV.o-chart-results-list__item')
                for i, cell in enumerate(data_cells):
                    row_data[f'data_cell_{i+1}'] = cell.text.strip() if cell else ''

            table_data.append(row_data)

        return table_data

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def output_to_google_sheets(data, spreadsheet_id, sheet_name):
    """
    Outputs the extracted data to a Google Sheet.

    Args:
        data: The list of dictionaries containing the table data.
        spreadsheet_id: The ID of the Google Sheet.
        sheet_name: The name of the sheet within the spreadsheet.
    """
    # Define the scope of the API
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
    ]

    # Access the Google service account key
    google_service_account_key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")
    if google_service_account_key_json:
        google_service_account_key = json.loads(google_service_account_key_json)
    else:
        print("GOOGLE_SERVICE_ACCOUNT_KEY not found.")
        exit()

    creds_dict = json.loads(google_service_account_key_json)
    creds = Credentials.from_service_account_info(creds_dict)

    # Authenticate with Google Sheets API
    client = gspread.authorize(creds)

    # Open the spreadsheet
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet '{sheet_name}' not found. Creating a new sheet.")
        sheet = client.open_by_key(spreadsheet_id).add_worksheet(title=sheet_name, rows="100", cols="20")
    except Exception as e:
        print(f"An error occurred while opening the spreadsheet: {e}")
        return

    # Prepare the data for output
    header = ['title', 'artist', 'data_cell_1', 'data_cell_2', 'data_cell_3', 'data_cell_4']
    output_data = [header]
    for row in data:
        output_row = [row.get('title', ''), row.get('artist', '')]
        for i in range(1, 5):
            output_row.append(row.get(f'data_cell_{i}', ''))
        output_data.append(output_row)

    # Update the sheet with the data
    try:
        sheet.update(output_data)
        print(f"Data successfully written to sheet '{sheet_name}'.")
    except Exception as e:
        print(f"An error occurred while writing to the sheet: {e}")

# Example usage:
url = 'https://www.billboard.com/artist/drake/chart-history/hot-100/'  # Replace with the actual URL of the page
spreadsheet_id = os.environ.get("GOOGLE_SPREADSHEET_ID")
if not spreadsheet_id:
    print("GOOGLE_SPREADSHEET_ID not found.")
    exit()
sheet_name = 'Sheet1'  # Replace with the desired sheet name

table_data = extract_table_data(url)

if table_data:
    output_to_google_sheets(table_data, spreadsheet_id, sheet_name)
else:
    print("No data extracted.")
