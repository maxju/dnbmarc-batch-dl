import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
from sqlalchemy.orm import Session
from model import DNBRecord, Session
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

def pretty_print_time(duration):
            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            duration = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"
            return duration

def download_and_save_file(id, url, download_dir, timeout=90): 
    try:
        start_time = datetime.now()
        # Request head
        response = requests.head(url, timeout=10, allow_redirects=True)
        file_size = int(response.headers.get("Content-Length", 0))
        logging.info(f"{id}: Downloading file with size {file_size / (1024 * 1024):.2f} MB from {url}")

        # Request file
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        file_extension = os.path.splitext(url)[1] or '.pdf'  # Default to .pdf if no extension
        file_name = f"{id}{file_extension}"
        file_path = os.path.join(download_dir, file_name)
        
        # Write file
        with open(file_path, 'wb') as file:
            file.write(response.content)

        return file_path
    except requests.RequestException as e:
        logging.error(f"Error downloading file from {url}: {e}")
        return None
def process_records(batch_size=1000):
    with Session() as session:
        start_time = datetime.now()
        base_query = select(DNBRecord).where(DNBRecord.url_dnb_archive.isnot(None), DNBRecord.path.is_(None))
        total_records = session.execute(select(func.count()).select_from(base_query.subquery())).scalar()
        print(f"Processing {total_records} records...")

        offset = 0
        while True:
            try:
                batch = session.execute(base_query.limit(batch_size).offset(offset)).scalars().all()
                if not batch:
                    break  # No more records to process

                for n, record in enumerate(batch, offset + 1):
                    time_taken = datetime.now() - start_time
                    print(f" ({n}/{total_records}), time taken: {pretty_print_time(time_taken)}")
                    
                    if record.path:
                        logging.info(f"Skipping record {record.id}: File already downloaded")
                        continue
                    
                    if record.url_dnb_archive:
                        logging.debug(f"Downloading file for record {record.id} from {record.url_dnb_archive}")
                        try:
                            file_path = download_and_save_file(record.id, record.url_dnb_archive, download_dir, timeout=60)
                            if file_path:
                                record.path = str(record.id)
                            else:
                                logging.warning(f"Failed to download file for record {record.id}")
                        except requests.exceptions.Timeout:
                            logging.warning(f"Download for record {record.id} timed out after 60 seconds.")
                        except Exception as e:
                            logging.error(f"An error occurred while downloading file for record {record.id}: {e}")
                    else:
                        logging.warning(f"No URL found for record {record.id}")
                
                session.commit()  # Commit after each batch
                offset += len(batch)

            except SQLAlchemyError as e:
                print(f"Database error: {e}")
                session.rollback()  # Rollback in case of database error
                break  # Exit the loop if there's a database error

    print(f"Finished processing. Total time: {pretty_print_time(datetime.now() - start_time)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    download_dir = os.getenv('DOWNLOAD_DIR') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/files')
    logging.info(f"Download directory: {download_dir}")
    os.makedirs(download_dir, exist_ok=True)
    process_records()

