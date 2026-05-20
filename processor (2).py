
import os
import requests

BASE_URL = "https://example.com/data"  # Replace with real source

def download_file(year, filename):
    url = f"{BASE_URL}/{year}/{filename}"
    os.makedirs(f"data/{year}", exist_ok=True)
    path = f"data/{year}/{filename}"

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded {filename} for {year}")
    except Exception as e:
        print(f"Error downloading {filename} for {year}: {e}")

def main(years):
    for y in years:
        download_file(y, "records.csv")
        download_file(y, "awa_suppliers.csv")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", required=True)
    args = parser.parse_args()
    main(args.years)
