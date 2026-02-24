# DisneyPlusExport2YamtrackImport
Bridge the gap between your Disney+ binge-watching sessions and your [**Yamtrack**](https://github.com/FuzzyGrim/Yamtrack) profile. This Python script takes your Disney+ data export, matches it against **The Movie Database (TMDB)**, and generates a pixel-perfect CSV file ready for bulk import.


## ✨ Features
* **Smart Matching:** Automatically identifies if a title is a Movie or a TV Show.
* **Hierarchy Handling:** Automatically creates the required `tv`, `season`, and `episode` rows for Yamtrack.
* **Title Cleaning:** Handles Disney+'s habit of adding extra text like ": Season 1" to titles before searching TMDB.
* **Error Logging:** Any title not found on TMDB is logged to `errors.log` for manual review.


## 🛠 Prerequisites

1. **Python 3.13+**
2. **TMDB API Key:** You’ll need a *Read Access Token*.


## 🚀 Setup & Usage

### 1. Prepare your Data
Convert the Disney+ export file from PDF to CSV. Place your Disney+ data export file in the script directory and rename it to `disney_plus_export.csv` (or update the filename in the script). Ensure it uses a semicolon (`;`) as a separator.

The contents of the file `disney_plus_export.csv` should look like this:
```csv
Profile ID;Program Title;Season Title;Date
my profile;Bluff;Prison Break;2026-01-01
```

### 2. Configure Environment
Create a `.env` file in the root folder:

```env
TMDB_API_READ_ACCESS_TOKEN=your_token_here

```

### 3. Run the Importer
```bash
uv run main.py
```

### 4. Import to Yamtrack
Once finished, a file named `yamtrack_import.csv` will appear. Head over to Yamtrack and upload this file to sync your history.


## ⚠️ Troubleshooting
* **"Series not found":** Check `errors.log`. Some titles on Disney+ differ slightly from the official database. You may need to edit the CSV manually for those rare cases.
* **Rate Limiting:** The script includes a small delay (`time.sleep`) to respect TMDB's API limits. If you have thousands of entries, grab a coffee—it might take a minute.
