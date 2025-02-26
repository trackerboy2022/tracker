import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_chart_data(url):
    """
    Scrapes the artist chart data from the given URL.

    Args:
        url (str): The URL of the web page to scrape.

    Returns:
        pandas.DataFrame: A DataFrame containing the scraped data, or None if an error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')
        rows = soup.find_all('div', class_='o-chart-results-list-row')

        data = []
        for row in rows:
            try:
                artist_song_div = row.find('div', class_='o-chart-results-list__item')
                artist_song = artist_song_div.get_text(strip=True) if artist_song_div else ''

                chart_data_div = row.find('div', class_='lrv-u-flex')
                chart_data = chart_data_div.get_text(strip=True) if chart_data_div else ''
                chart_data_values = chart_data.split()
                date = chart_data_values[0] if len(chart_data_values) > 0 else ''
                rank = chart_data_values[1] if len(chart_data_values) > 1 else ''
                weeks_on_chart = f"{chart_data_values[2]} {chart_data_values[3]}" if len(chart_data_values) > 3 else ''
                peak_position = chart_data_values[-1] if len(chart_data_values) > 0 else ''

                data.append([artist_song, date, rank, weeks_on_chart, peak_position])
            except Exception as row_error:
                print(f"Error extracting data from a row: {row_error}")

        df = pd.DataFrame(data, columns=['Artist/Song', 'Date', 'Rank', 'Weeks on Chart', 'Peak Position'])
        return df

    except requests.exceptions.RequestException as req_error:
        print(f"Error during request: {req_error}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def main():
    """
    Main function to scrape the data and save it to a CSV file.
    """
    url = 'https://www.billboard.com/artist/drake/chart-history/hot-100/'  # Replace with the actual URL
    output_file = 'chart_data.csv'

    df = scrape_chart_data(url)

    if df is not None:
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()
