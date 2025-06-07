import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from rapidfuzz import process, fuzz  # Import fuzz separately
from espn_api.baseball import League
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

def extract_pitcher_rankings():
    """
    Extracts the latest Starting Pitcher Streamer Rankings from Pitcher List's WordPress API,
    matches them with ESPN Fantasy Baseball players, and exports the data to a Google Sheet.
    """

    # Step 1: Access the WordPress API
    url = 'https://pitcherlist.com/wp-json/wp/v2/posts?per_page=20'
    response = requests.get(url)
    response.raise_for_status()
    posts = response.json()

    # Step 2: Find the latest Starting Pitcher Rankings post
    target_post = None
    for post in posts:
        title = post['title']['rendered']
        if "Starting Pitcher Streamer Rankings" in title:
            target_post = post
            break

    if not target_post:
        raise ValueError("No Starting Pitcher Rankings post found!")

    # Step 3: Parse the content
    html_content = target_post['content']['rendered']
    soup = BeautifulSoup(html_content, 'html.parser')

    # Step 4: Extract the rankings with tiers and dates
    rankings = []
    start_collecting = False
    current_tier = None

    # Define tiers to look for
    tier_keywords = {
        "Auto-Starts": "Auto-Start",
        "Probably Starts": "Probably Start",
        "Questionable Starts": "Questionable Start",
        "Do Not Starts": "Do Not Start"
    }

    for p_tag in soup.find_all('p'):
        text = p_tag.get_text(strip=True)

        # Detect when rankings actually start
        if re.search(r"Starting Pitcher Streamer Rankings", text, re.I):
            start_collecting = True
            continue

        if start_collecting:
            # Check if this paragraph is a tier heading
            for key in tier_keywords.keys():
                if re.search(key, text, re.I):
                    current_tier = tier_keywords[key]
                    break

            # Otherwise, look for player entries
            strong_tag = p_tag.find('strong')
            if strong_tag:
                player_link = strong_tag.find('a', class_='player-tag')
                if player_link:
                    player_name = player_link.get_text(strip=True)

                    # Find opponent
                    after_player = strong_tag.get_text()
                    opponent_match = re.search(r'vs\. ([A-Z]+)|@ ([A-Z]+)', after_player)
                    if opponent_match:
                        opponent = opponent_match.group(1) or opponent_match.group(2)
                    else:
                        opponent = None

                    # Blurb text (after the strong tag)
                    blurb = p_tag.get_text().replace(strong_tag.get_text(), '').strip(' –—')

                    # Add the extracted data to the rankings list
                    rankings.append({
                        'Tier': current_tier,
                        'Player': player_name,
                        'Opponent': opponent,
                        'Blurb': blurb
                    })

    # Step 5: Output
    df = pd.DataFrame(rankings)

    print(f"Post Title: {target_post['title']['rendered']}")
    print(f"Post URL: {target_post['link']}")

    return df, rankings

df, rankings = extract_pitcher_rankings()

# Step 6: Connect to your ESPN Fantasy Baseball League
league_id = os.getenv("SECRET_LEAGUE_ID")
season_id = 2025
espn_s2 = os.getenv("SECRET_ESPN_S2_COOKIE")
swid = os.getenv("SECRET_SWID")

def espn_fuzzy_match():
    # Authenticate and connect to the league
    league = League(league_id=league_id, year=season_id, espn_s2=espn_s2, swid=swid)

    # Step 7: Fetch available players
    available_players = league.free_agents(size=500, position='P')

    # Step 8: Filter for pitchers
    # Check if the player is a pitcher by looking for 'SP' or 'RP' in eligibleSlots
    available_pitchers = [
        player for player in available_players
        if 'P' in player.eligibleSlots
    ]

    # Step 9: Compare with rankings using exact and fuzzy matching
    available_ranked_pitchers = []

    # Extract player names from rankings
    ranked_player_names = [ranking['Player'] for ranking in rankings]

    # Perform exact and fuzzy matching
    for pitcher in available_pitchers:
        # Exact match
        if pitcher.name in ranked_player_names:
            match_name = pitcher.name  # Exact match
        else:
            # Fuzzy match
            match, score, _ = process.extractOne(
                pitcher.name, ranked_player_names, scorer=fuzz.ratio
            )
            if score >= 85:  # Adjust threshold as needed
                match_name = match  # Fuzzy matched name
            else:
                match_name = None  # No match found

        # If a match is found, update the rankings DataFrame
        if match_name:
            df.loc[df['Player'] == match_name, 'ESPN Name'] = pitcher.name
        
    return df

df = espn_fuzzy_match()

creds_json = os.environ.get("GOOGLE_SHEETS_CREDS")

def get_google_client():
    """Set up and return authenticated Google Sheets client."""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(creds_json), 
            scope
        )
    
    # Try to get credentials from environment variable first
    # if creds_json:
    #     creds = ServiceAccountCredentials.from_json_keyfile_dict(
    #         json.loads(creds_json), 
    #         scope
    #     )
    # # If not found, use the local JSON file
    # else:
    #     if not os.path.exists(creds_file):
    #         raise FileNotFoundError("Google Sheets credentials not found")
    #     creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    
    return gspread.authorize(creds)

def import_google_sheet(sheet_id, sheet_name):
    """Import data from Google Sheet using sheet ID."""
    client = get_google_client()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    return sheet.get_all_records()

SOURCE_SHEET_ID = "15yyCk5HEIUbWMMyVC3-P-UorLXUT52eiIp3lD2ST1TA"
google_sheet_data = import_google_sheet(SOURCE_SHEET_ID, "Ranks 4/25")

def process_google_sheet_data(df, google_sheet_data):
    """
    Process Google Sheets data and update the rankings DataFrame with fuzzy matching.
    Args:
        df: DataFrame containing the rankings data.
        google_sheet_data: List of dictionaries containing Google Sheets data.
    Returns:
        Updated DataFrame with Google Sheets data merged.
    """

    # Step 11: Perform fuzzy matching on Google Sheets data and update the DataFrame
    google_sheet_df = pd.DataFrame(google_sheet_data)  # Convert Google Sheets data to a DataFrame

    # Ensure the Google Sheets DataFrame has only the required columns
    google_sheet_df = google_sheet_df[['Eno', 'Name', 'Stuff+', 'Location+', 'Pitching+', 'Blurb']]

    # Rename the 'Blurb' column to 'Notes'
    google_sheet_df.rename(columns={'Blurb': 'Notes'}, inplace=True)
    google_sheet_df.rename(columns={'Name': 'Eno Name'}, inplace=True)

    # Perform fuzzy matching between rankings and Google Sheets data
    for index, row in df.iterrows():
        player_name = row['Player']  # Player name from rankings DataFrame

        # Fuzzy match with the 'Name' column in Google Sheets data
        match, score, _ = process.extractOne(
            player_name, google_sheet_df['Eno Name'], scorer=fuzz.ratio
        )
        if score >= 85:  # Adjust threshold as needed
            # Get the matching row from Google Sheets data
            matched_row = google_sheet_df[google_sheet_df['Eno Name'] == match].iloc[0]

            # Add the Google Sheets data to the rankings DataFrame
            df.loc[index, 'Eno Name'] = matched_row['Eno Name']
            df.loc[index, 'Eno'] = matched_row['Eno']
            df.loc[index, 'Stuff+'] = matched_row['Stuff+']
            df.loc[index, 'Location+'] = matched_row['Location+']
            df.loc[index, 'Pitching+'] = matched_row['Pitching+']
            df.loc[index, 'Notes'] = matched_row['Notes']

    # Reorder the DataFrame columns
    column_order = [
        'Player', 'Eno Name', 'ESPN Name', 'Tier', 'Opponent', 'Blurb',
        'Eno', 'Stuff+', 'Location+', 'Pitching+', 'Notes'
    ]
    df = df[column_order]

    # Round the specified columns to whole numbers
    columns_to_round = ['Eno', 'Stuff+', 'Location+', 'Pitching+']

    # Convert columns to numeric, coercing invalid values to NaN
    df[columns_to_round] = df[columns_to_round].apply(pd.to_numeric, errors='coerce')

    # Fill NaN values with 0 (or another default value)
    df[columns_to_round] = df[columns_to_round].fillna(0)

    # Round the columns to whole numbers and convert to integers
    df[columns_to_round] = df[columns_to_round].round(0).astype(int)

    # Replace 0 with an empty string
    df[columns_to_round] = df[columns_to_round].replace(0, '')

    # Filter out rows where Tier is "Do Not Start"
    df = df[df['Tier'] != 'Do Not Start']

    return df

df = process_google_sheet_data(df, google_sheet_data)

def export_to_google_sheet(df, sheet_id, sheet_name):
    """Export DataFrame to Google Sheet using sheet ID."""
    client = get_google_client()
    
    try:
        spreadsheet = client.open_by_key(sheet_id)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(sheet_name, rows=1000, cols=26)
        
        # Prepare and export data
        df_export = df.copy().fillna('').astype(str)
        data_to_export = [df_export.columns.values.tolist()] + df_export.values.tolist()
        
        worksheet.clear()
        worksheet.update(data_to_export, value_input_option='RAW')
        print(f"Data successfully exported to {sheet_name}")
        
    except Exception as e:
        print(f"Error exporting to Google Sheet: {str(e)}")
        raise


TARGET_SHEET_ID = os.getenv("SECRET_SPREADSHEET_ID")
export_to_google_sheet(df, TARGET_SHEET_ID, 'Sheet2')

print("Success")
