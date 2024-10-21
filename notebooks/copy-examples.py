import sqlite3
import json
from collections import defaultdict
import os
import shutil
from datetime import datetime, timedelta
import random
import logging


def load_ddc_basic():
    ddc_basic_path = "../data/ddc/ddc-basic.json"
    with open(ddc_basic_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_german_category(ddc_number, ddc_basic):
    main_category = ddc_number[0]
    rounded_sub_category = ddc_number[:3].rjust(
        3, "0"
    )  # Ensure 3 digits, pad with zeros if needed
    rounded_sub_category = (
        f"{int(rounded_sub_category) // 10 * 10:03d}"  # Round down to nearest ten
    )

    if main_category in ddc_basic:
        main_name = ddc_basic[main_category]["name"]
        for key, value in ddc_basic[main_category]["sub"].items():
            if "-" in key:
                start, end = map(int, key.split("-"))
                if start <= int(rounded_sub_category) <= end:
                    return main_category, main_name, f"{key} {value['name']}"
            elif key == rounded_sub_category:
                return main_category, main_name, f"{key} {value['name']}"
        return (
            main_category,
            main_name,
            f"{rounded_sub_category} Unterkategorie nicht gefunden",
        )
    else:
        return "0", "Kategorie nicht gefunden", "000 Unterkategorie nicht gefunden"


def sanitize_filename(filename):
    return "".join(c if c.isalnum() or c in ["-", "_"] else "_" for c in filename)


def select_diverse_pdfs(cursor, main_category, sub_category, ddc_number):
    current_year = datetime.now().year
    three_years_ago = current_year - 2  # This will give us the last 3 years

    # Query for recent PDFs (last 3 years)
    recent_query = """
    SELECT path, year 
    FROM dnb_records 
    WHERE ddc LIKE ? AND path LIKE '%.pdf' AND year >= ?
    ORDER BY RANDOM()
    LIMIT 5
    """
    cursor.execute(recent_query, (f"{ddc_number}%", three_years_ago))
    recent_results = cursor.fetchall()

    # Query for older PDFs
    older_query = """
    SELECT path, year 
    FROM dnb_records 
    WHERE ddc LIKE ? AND path LIKE '%.pdf' AND year < ?
    ORDER BY RANDOM()
    LIMIT 5
    """
    cursor.execute(older_query, (f"{ddc_number}%", three_years_ago))
    older_results = cursor.fetchall()

    # Combine and shuffle the results
    combined_results = recent_results + older_results
    random.shuffle(combined_results)

    return combined_results


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the SQLite database
conn = sqlite3.connect("../downloader/dnb_records.db")
cursor = conn.cursor()

# Load DDC basic data
ddc_basic = load_ddc_basic()

# Get the download directory from environment variables
download_dir = os.getenv('DOWNLOAD_DIR') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/files')

# Group results and select diverse PDFs
grouped_results = defaultdict(lambda: defaultdict(list))

query = "SELECT ddc FROM dnb_records WHERE ddc IS NOT NULL AND ddc != ''"
cursor.execute(query)
results = cursor.fetchall()

for row in results:
    ddc_number = row[0][:3]
    main_category, main_name, sub_category = get_german_category(ddc_number, ddc_basic)

    if not grouped_results[f"{main_category} {main_name}"][sub_category]:
        selected_pdfs = select_diverse_pdfs(
            cursor, main_category, sub_category, ddc_number
        )
        grouped_results[f"{main_category} {main_name}"][sub_category] = selected_pdfs

# Log the grouped results and copy files
for main_category, sub_categories in sorted(
    grouped_results.items(), key=lambda x: x[0].split()[0]
):
    # logging.info(f"\n{main_category}")

    main_folder = sanitize_filename(
        f"{main_category.split()[0]}_{main_category.split()[1]}"
    )

    for sub_category, pdfs in sorted(
        sub_categories.items(),
        key=lambda x: x[0].split()[0] if x[0].split()[0].isdigit() else "999",
    ):
        # logging.info(f"  {sub_category}")

        sub_folder = sanitize_filename(
            f"{sub_category.split()[0]}_{' '.join(sub_category.split()[1:])}"
        )
        target_folder = os.path.join("examples", main_folder, sub_folder)

        # Uncomment the following lines when you want to create the directories
        os.makedirs(target_folder, exist_ok=True)
        # logging.info(f"Created folder: {target_folder}")

        for pdf_path, year in pdfs:
            original_filename = os.path.basename(pdf_path)
            # new_filename = f"{year}_{original_filename}"
            # logging.info(f"    {new_filename}")
            # Copy the file
            source_path = os.path.join(download_dir, pdf_path)
            shutil.copy2(source_path, os.path.join(target_folder, new_filename))
            logging.info(f"Copied file: {pdf_path} to {target_folder}/{new_filename}")

# Close the connection
conn.close()