
import os
import requests

BASE_URL = "https://example.com/data"  # Replace with real API

def download_file(year, filename):
    os.makedirs(f"data/raw/{year}", exist_ok=True)
    path = f"data/raw/{year}/{filename}"
    url = f"{BASE_URL}/{year}/{filename}"

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded {filename} for {year}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")

def main(years):
    for y in years:
        download_file(y, "records.csv")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", required=True)
    args = parser.parse_args()
    main(args.years)
