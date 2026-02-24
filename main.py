import os
import time
import logging
import requests
import pandas
from dotenv import load_dotenv

load_dotenv()

TMDB_API_READ_ACCESS_TOKEN = os.getenv("TMDB_API_READ_ACCESS_TOKEN", "none")
INPUT_FILE = "disney_plus_export.csv"
OUTPUT_FILE = "yamtrack_import.csv"
LOG_FILE = "errors.log"

# Logging Setup
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TMDBClient:
    def __init__(self, api_token:str):
        self._api_token:str = api_token
        self._base_url:str = "https://api.themoviedb.org/3"
        self._headers:dict = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json;charset=utf-8"
        }

    def search_movie(self, title):
        url:str = f"{self._base_url}/search/movie"
        params:dict = {"query": title, "language": "en-US"}
        response = requests.get(url, headers=self._headers, params=params)
        results = response.json().get("results", [])
        return results[0] if results else None

    def search_tv_show(self, show_name):
        url:str = f"{self._base_url}/search/tv"
        params:dict = {"query": show_name, "language": "en-US"}
        response = requests.get(url, headers=self._headers, params=params)
        results = response.json().get("results", [])
        return results[0] if results else None

    def get_episode_info(self, tv_id, episode_title):
        show_url = f"{self._base_url}/tv/{tv_id}"
        show_data = requests.get(show_url, headers=self._headers).json()

        # Iterate through seasons to find episode
        for season in show_data.get("seasons", []):
            season_number = season["season_number"]
            season_url = f"{self._base_url}/tv/{tv_id}/season/{season_number}"
            season_data = requests.get(season_url, headers=self._headers).json()

            for ep in season_data.get("episodes", []):
                # Comparison of titles
                if ep["name"].lower() == episode_title.lower():
                    return season_number, ep["episode_number"]
        return None, None

def process_disney_data():
    try:
        df = pandas.read_csv(INPUT_FILE, sep=";")
    except FileNotFoundError:
        print(f"Error: file {INPUT_FILE} not found.")
        return

    # Convert date into datetime format
    df["Date"] = pandas.to_datetime(df["Date"])

    # Sort entries by date
    df = df.sort_values(by="Date")

    # Deduplication: keep only last line
    df = df.drop_duplicates(subset=["Program Title", "Season Title"], keep="last")

    # Convert date back to string
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    yamtrack_rows = []
    processed_tv_shows = set() # To create season entries only once
    processed_seasons = set()

    tmdb = TMDBClient(api_token=TMDB_API_READ_ACCESS_TOKEN)

    for index, row in df.iterrows():
        prog_title = str(row["Program Title"]) if pandas.notna(row["Program Title"]) else ""
        seas_title = str(row["Season Title"]) if pandas.notna(row["Season Title"]) else ""
        date_str = str(row["Date"])

        # set timestamp to 14:14 mez
        end_date = f"{date_str} 14:14:00+01:00"

        # case 1: error (season without episode title)
        if not prog_title and seas_title:
            logging.warning(f"Skipped (missing episode title): {seas_title} on {date_str}. Please check manually.")
            continue

        # case 2: season (season and episode title)
        elif prog_title and seas_title:
            show_info = tmdb.search_tv_show(seas_title)
            if show_info:
                tv_id = show_info["id"]
                s_num, e_num = tmdb.get_episode_info(tv_id, prog_title)

                if s_num is not None:
                    # TV entry (once per series)
                    if tv_id not in processed_tv_shows:
                        yamtrack_rows.append({
                            "media_id": tv_id, "source": "tmdb", "media_type": "tv",
                            "status": "In progress", "end_date": ""
                        })
                        processed_tv_shows.add(tv_id)

                    # Season entry (once per season)
                    season_key = f"{tv_id}_{s_num}"
                    if season_key not in processed_seasons:
                        yamtrack_rows.append({
                            "media_id": tv_id, "source": "tmdb", "media_type": "season",
                            "season_number": s_num, "status": "In progress", "end_date": ""
                        })
                        processed_seasons.add(season_key)

                    # Episode entry (once per episode)
                    yamtrack_rows.append({
                        "media_id": tv_id, "source": "tmdb", "media_type": "episode",
                        "season_number": s_num, "episode_number": e_num,
                        "status": "Completed", "end_date": end_date
                    })
                else:
                    logging.error(f"Didn't find episode '{prog_title}' of season '{seas_title}' on TMDB.")
            else:
                logging.error(f"Didn't find series '{seas_title}' on TMDB.")

        # case 3: film (only program title filled)
        elif prog_title and not seas_title:
            movie_info = tmdb.search_movie(prog_title)
            if movie_info:
                yamtrack_rows.append({
                    "media_id": movie_info['id'], "source": "tmdb", "media_type": "movie",
                    "status": "Completed", "end_date": end_date
                })
            else:
                logging.error(f"Didn't find movie '{prog_title}' on TMDB.")

        # Rate Limiting für API
        time.sleep(0.1)

    output_df = pandas.DataFrame(yamtrack_rows)

    # Ensure all columns meet specifications
    columns = ["media_id", "source", "media_type", "title", "image", "season_number",
               "episode_number", "score", "status", "notes", "start_date", "end_date", "progress"]

    for col in columns:
        if col not in output_df.columns:
            output_df[col] = ""

    # Convert season and episode numbers to string
    for col in ["season_number", "episode_number"]:
        output_df[col] = pandas.to_numeric(output_df[col], errors="coerce").astype("Int64").astype(str)
        output_df[col] = output_df[col].replace("<NA>", "")

    output_df[columns].to_csv(OUTPUT_FILE, index=False, quoting=1)
    print(f"Done! Import file '{OUTPUT_FILE}' was created. See errors at '{LOG_FILE}'.")

if __name__ == "__main__":
    process_disney_data()
