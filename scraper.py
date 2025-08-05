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

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/',  # Simulate coming from a search engine
            'Connection': 'keep-alive'
        }
    
        url = 'https://www.billboard.com/artist/drake/chart-history/hot-100/'
        response = requests.get(url, headers=headers)
    
        if response.status_code == 200:
            print("Request successful!")
            # Process the HTML content
        else:
            print(f"Request failed with status code: {response.status_code}")

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

            # Find all the data cell containers within the row
            data_cells = row.select('DIV.o-chart-results-list__item')

            # Ensure you have enough data cells
            if len(data_cells) >= 5: # We now expect at least 5 data cells
                # The first cell contains the title and artist
                # You can still extract them like this, or from data_cells[0]
                title_element = row.select_one('H3#title-of-a-story.c-title')
                row_data['title'] = title_element.text.strip() if title_element else ''

                artist_element = row.select_one('SPAN.c-label')
                row_data['artist'] = artist_element.text.strip() if artist_element else ''

                # Extract Debut Date from the second data cell (index 1)
                debut_date_cell = data_cells[1]
                debut_date_element = debut_date_cell.select_one('SPAN.artist-chart-row-debut-date')
                row_data['debut_date'] = debut_date_element.text.strip() if debut_date_element else ''

                # Extract Peak Position from the third data cell (index 2)
                peak_pos_cell = data_cells[2]
                peak_pos_element = peak_pos_cell.select_one('SPAN.artist-chart-row-peak-pos')
                peak_week_element = peak_pos_cell.select_one('SPAN.artist-chart-row-peak-week')
                peak_position = peak_pos_element.text.strip() if peak_pos_element else ''
                peak_week = peak_week_element.text.strip() if peak_week_element else ''
                row_data['peak_position'] = f"{peak_position}\n\n{peak_week}" if peak_position or peak_week else ''

                # Extract Peak Date from the fourth data cell (index 3)
                peak_date_cell = data_cells[3]
                peak_date_element = peak_date_cell.select_one('SPAN.artist-chart-row-peak-date')
                row_data['peak_date'] = peak_date_element.text.strip() if peak_date_element else ''

                # Extract Weeks on Chart from the fifth data cell (index 4)
                weeks_on_chart_cell = data_cells[4]
                weeks_on_chart_element = weeks_on_chart_cell.select_one('SPAN.artist-chart-row-week-on-chart')
                row_data['weeks_on_chart'] = weeks_on_chart_element.text.strip() if weeks_on_chart_element else ''

                # Note: The third cell also contains 'peak-week'. If you need that, you can extract it similarly:
                # peak_week_element = peak_pos_cell.select_one('SPAN.artist-chart-row-peak-week')
                # row_data['peak_week'] = peak_week_element.text.strip() if peak_week_element else ''

            else:
                print(f"Warning: Not enough data cells found in row: {row_data.get('title', 'Unknown Title')}")

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
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]

    # Access the Google service account key
    google_service_account_key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")
    if google_service_account_key_json:
        google_service_account_key = json.loads(google_service_account_key_json)
    else:
        print("GOOGLE_SERVICE_ACCOUNT_KEY not found.")
        exit()

    creds = Credentials.from_service_account_info(google_service_account_key, scopes=scopes)

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
    header = ['title', 'artist', 'debut_date', 'peak_position', 'peak_date', 'weeks_on_chart']
    output_data = [header]
    for row in data:
        output_row = [
            row.get('title', ''),
            row.get('artist', ''),
            row.get('debut_date', ''),
            row.get('peak_position', ''),
            row.get('peak_date', ''),
            row.get('weeks_on_chart', '')
        ]
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
