import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session
import requests
from model import engine, DNBRecord, Session as ModelSession
from datetime import datetime
import time
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from PyPDF2 import PdfReader
from io import BytesIO

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
        content_type = response.headers.get("Content-Type", "")
        logging.info(f"{id}: Downloading {content_type} file with size {file_size / (1024 * 1024):.2f} MB from {url}")

        # Request file
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        # file handling
        file_extension = os.path.splitext(url)[1] or '.pdf'  # Default to .pdf if no extension
        file_name = f"{id}{file_extension}"
        file_path = os.path.join(download_dir, file_name)


        # Write file
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # Get number of pages
        try:
            num_pages = 0
            pdf = PdfReader(BytesIO(response.content))
            num_pages = len(pdf.pages)
        except Exception as e:
            logging.warning(f"Could not determine number of pages for {file_name}: {e}")

        return file_name, file_size, content_type, file_extension, num_pages

    except requests.RequestException as e:
        logging.error(f"Error downloading file from {url}: {e}")
        return None

def process_record(record_id, url_dnb_archive, download_dir, session_factory):
    """Worker function to process a single record."""
    session = session_factory()
    try:

        # Check if the file is already downloaded
        record = session.get(DNBRecord, record_id)
        if not record:
            logging.error(f"Record not found: {record_id}")
            return

        if record.path:
            logging.info(f"Skipping record {record_id}: File already downloaded")
            return

        if url_dnb_archive:
            try:
                file_name, file_size, content_type, file_extension, num_pages = download_and_save_file(record.idn, url_dnb_archive, download_dir, timeout=60)
                if file_name:
                    record.path = file_name
                    record.file_size = file_size
                    record.content_type = content_type
                    record.file_extension = file_extension
                    record.num_pages = num_pages
                    session.commit()
                    logging.info(f"Successfully downloaded and updated record {record_id}")
                else:
                    logging.warning(f"Failed to download file for record {record_id}")
            except requests.exceptions.Timeout:
                logging.warning(f"Download for record {record_id} timed out after 60 seconds.")
            except Exception as e:
                logging.error(f"An error occurred while downloading file for record {record_id}: {e}")
        else:
            logging.warning(f"No URN found for record {record_id}")
    except SQLAlchemyError as e:
        logging.error(f"Database error for record {record_id}: {e}")
        session.rollback()
    finally:
        session.close()

def process_records(download_dir, max_concurrent_downloads=20, batch_size=1000):
    start_time = datetime.now()

    Session = scoped_session(ModelSession)

    try:
        records_with_url = Session().query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None)
        ).scalar()

        records_with_file = Session().query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None),
            DNBRecord.path.isnot(None)
        ).scalar()

        records_to_process = Session().query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None),
            DNBRecord.path.is_(None)
        ).scalar()

        print(f"Not processing {records_with_file} records with file already downloaded...")
        print(f"Processing {records_to_process} records...")

        completed_records = records_with_file
        last_progress_update = time.time()

        with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
            active_futures = deque()
            records_generator = get_records(Session, batch_size)

            while True:
                # Start new futures if we have less than max_concurrent_downloads
                while len(active_futures) < max_concurrent_downloads:
                    try:
                        record = next(records_generator)
                        future = executor.submit(
                            process_record,
                            record_id=record.id,
                            url_dnb_archive=record.url_dnb_archive,
                            download_dir=download_dir,
                            session_factory=Session
                        )
                        active_futures.append(future)
                    except StopIteration:
                        # No more records to process
                        break

                if not active_futures:
                    # No more active futures and no more records to process
                    break

                # Wait for the next future to complete
                completed_future = next(as_completed(active_futures))
                active_futures.remove(completed_future)

                try:
                    completed_future.result()
                    completed_records += 1

                    # Update progress every 1 second
                    current_time = time.time()
                    if current_time - last_progress_update >= 1:
                        progress = (completed_records / records_with_url) * 100
                        print(f"\rProgress: {progress:.2f}% ({completed_records}/{records_to_process})", end="", flush=True)
                        last_progress_update = current_time
                except Exception as e:
                    logging.error(f"Exception in worker thread: {e}")

        print(f"\rProgress: 100.00% ({records_to_process}/{records_to_process})")

    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
    finally:
        Session.remove()

    total_time = datetime.now() - start_time
    print(f"Finished processing. Total time: {pretty_print_time(total_time)}")

def get_records(Session, batch_size):
    """Generator function to yield records in batches."""
    offset = 0
    while True:
        records = Session().query(DNBRecord).filter(
            DNBRecord.url_dnb_archive.isnot(None),
            DNBRecord.path.is_(None)
        ).order_by(DNBRecord.year.desc()).offset(offset).limit(batch_size).all()
        
        if not records:
            break
        
        for record in records:
            yield record
        
        offset += batch_size

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    download_dir = os.getenv('DOWNLOAD_DIR') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/files')
    os.makedirs(download_dir, exist_ok=True)
    logging.info(f"Download directory: {download_dir}")
    logging.getLogger('').setLevel(logging.WARNING)

    process_records(download_dir=download_dir, max_concurrent_downloads=12, batch_size=1000)