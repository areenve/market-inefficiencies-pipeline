# scripts/prepare_data.py
from market_inefficiencies.io import ensure_project_dirs, unzip_to_raw
from market_inefficiencies.paths import DATA_RAW

def main():
    ensure_project_dirs()

    # Put the zip file here: data/raw/cross_venue_dislocations.zip
    zip_path = DATA_RAW / "cross_venue_dislocations.zip"
    extracted = unzip_to_raw(zip_path, folder_name="cross_venue_dislocations")
    print(f"Extracted to: {extracted}")

if __name__ == "__main__":
    main()
