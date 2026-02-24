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
        self._show_cache:dict = {}

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
        if tv_id not in self._show_cache:
            show_url = f"{self._base_url}/tv/{tv_id}"
            show_data = requests.get(show_url, headers=self._headers).json()

            all_episodes:dict = {}

            # Iterate through seasons to find episode
            for season in show_data.get("seasons", []):
                season_number = season["season_number"]
                season_url = f"{self._base_url}/tv/{tv_id}/season/{season_number}"
                season_data = requests.get(season_url, headers=self._headers).json()

                for episode in season_data.get("episodes", []):
                    all_episodes[episode["name"].lower().strip()] = (season_number, episode["episode_number"])

            self._show_cache[tv_id] = all_episodes

        # Search in cache
        return self._show_cache[tv_id].get(episode_title.lower().strip(), (None, None))

def process_disney_data():
    try:
        # Open input file
        df = pandas.read_csv(INPUT_FILE, sep=";")

    except FileNotFoundError:
        print(f"Error: file {INPUT_FILE} not found.")
        return

    # Convert column End Date into datetime format
    df["End Date"] = pandas.to_datetime(df["End Date"], format="%d/%m/%Y")

    # Convert column End Time into datetime format
    df["End Time"] = pandas.to_datetime(df["End Time"], format="%H:%M:%S")

    # Sort entries by end date and end time
    df = df.sort_values(by=["End Date", "End Time"])

    # Deduplication: keep only last line
    df = df.drop_duplicates(subset=["Program Title", "Season Title"], keep="last")

    # Convert Date back to string
    df["End Date"] = df["End Date"].dt.strftime("%Y-%m-%d")

    # Convert Time back to string
    df["End Time"] = df["End Time"].dt.strftime("%H:%M:%S")

    yamtrack_rows:list = []
    processed_tv_shows:set = set() # To create tv show entries only once
    processed_seasons:set = set() # To create season entries only once

    tmdb = TMDBClient(api_token=TMDB_API_READ_ACCESS_TOKEN)

    for index, row in df.iterrows():
        # Get properties of current row
        program_title = str(row["Program Title"]).strip() if pandas.notna(row["Program Title"]) else ""
        season_title = str(row["Season Title"]).strip() if pandas.notna(row["Season Title"]) else ""
        end_date = row["End Date"].strip()
        end_time = str(row["End Time"]).strip()
        end_datetime = f"{end_date} {end_time}+00:00"

        # case 1: error (season_title existing episode_title missing)
        if not program_title and season_title:
            logging.warning(f"Skipped (missing episode title): {season_title} on {end_datetime}. Please check manually.")
            continue

        # case 2: series (season_title and episode_title existing)
        elif program_title and season_title:
            show_info = tmdb.search_tv_show(season_title)
            if show_info:
                tv_id = show_info["id"]
                season_number, episode_number = tmdb.get_episode_info(tv_id, program_title)
                time.sleep(0.05)

                if season_number is not None:
                    # TV entry (once per series)
                    if tv_id not in processed_tv_shows:
                        yamtrack_rows.append(
                            {
                                "media_id": tv_id,
                                "source": "tmdb",
                                "media_type": "tv",
                                "status": "In progress",
                                "end_date": ""
                            }
                        )
                        processed_tv_shows.add(tv_id)

                    # Season entry (once per season)
                    season_key = f"{tv_id}_{season_number}"
                    if season_key not in processed_seasons:
                        yamtrack_rows.append(
                            {
                                "media_id": tv_id,
                                "source": "tmdb",
                                "media_type": "season",
                                "season_number": season_number,
                                "status": "In progress",
                                "end_date": ""
                            }
                        )
                        processed_seasons.add(season_key)

                    # Episode entry (once per episode)
                    yamtrack_rows.append(
                        {
                            "media_id": tv_id,
                            "source": "tmdb",
                            "media_type": "episode",
                            "season_number": season_number,
                            "episode_number": episode_number,
                            "status": "Completed",
                            "end_date": end_datetime,
                        }
                    )
                else:
                    logging.error(f"Didn't find episode '{program_title}' of season '{season_title}' on TMDB. Watched on: {end_datetime}. Please check manually.")
            else:
                logging.error(f"Didn't find series '{season_title}' on TMDB. Watches on {end_datetime}. Please check manually.")

        # case 3: movie (program_title existing season_title missing)
        elif program_title and not season_title:
            movie_info = tmdb.search_movie(program_title)
            if movie_info:
                yamtrack_rows.append(
                    {
                        "media_id": movie_info["id"],
                        "source": "tmdb",
                        "media_type": "movie",
                        "status": "Completed",
                        "end_date": end_datetime
                    }
                )
            else:
                logging.error(f"Didn't find movie '{program_title}' on TMDB. Watched on {end_datetime}. Please check manually.")

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
