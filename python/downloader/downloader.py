import logging
import os
import sys
from sqlalchemy import create_engine, Column, String, Integer, Sequence, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session
import requests
from datetime import datetime, timedelta
import time
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import fitz  # PyMuPDF
import gc
import tempfile
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, init_db, get_session, DNBRecord


# Create a scoped session factory
engine = get_engine() # Create tables if they don't exist
SessionFactory = scoped_session(sessionmaker(bind=engine))

def pretty_print_time(duration):
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h{int(minutes)}m{int(seconds)}s"

def get_pdf_pages(file_path):
    num_pages = 0
    pdf = None

    try:
        pdf = fitz.open(file_path)
        num_pages = len(pdf)
    except Exception as e:
        logging.warning(f"Error processing PDF: {e}")
    finally:
        if pdf:
            pdf.close()
            del pdf
        if num_pages == 0:
            logging.warning(f"Could not determine any readable pages")
        else:
            logging.info(f"Successfully read {num_pages} pages")

        gc.collect()

    return num_pages

def download_and_save_file(id, url, download_dir, timeout=90):
    temp_file_path = None
    try:
        start_time = datetime.now()

        # Request file
        with requests.get(url, timeout=timeout, allow_redirects=True, stream=True) as response:
            response.raise_for_status()

            file_size = int(response.headers.get("Content-Length", 0))
            content_type = response.headers.get("Content-Type", "")
            

            # file handling
            file_extension = os.path.splitext(url)[1] or '.pdf'  # Default to .pdf if no extension
            file_name = f"{id}{file_extension}"
            final_file_path = os.path.join(download_dir, file_name)

            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, dir=download_dir, prefix=f"{id}_temp_", suffix=file_extension) as temp_file:
                temp_file_path = temp_file.name
                # Write file in chunks
                for chunk in response.iter_content(chunk_size=8192): 
                    if chunk:
                        temp_file.write(chunk)
                temp_file.flush()
                os.fsync(temp_file.fileno())  # Ensure all data is written to disk

        shutil.move(temp_file_path, final_file_path)
        logging.info(f"{id}: Downloaded {content_type} file with size {file_size / (1024 * 1024):.2f} MB from {url} to {download_dir}/{file_name}")

        num_pages = get_pdf_pages(final_file_path)

        return file_name, file_size, content_type, file_extension, num_pages

    except requests.RequestException as e:
        logging.error(f"Error downloading file from {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error while downloading and saving file: {e}")
        return None
    finally:
        # Clean up temporary file if it exists
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logging.error(f"Error deleting temporary file {temp_file_path}: {e}")
        
        # Force garbage collection
        del response
        gc.collect()

def process_record(record_id, url_dnb_archive, download_dir, session_factory):
    """Worker function to process a single record."""

    session = SessionFactory()
    try:
        # Check if the file is already downloaded
        record = session.get(DNBRecord, record_id)
        if not record:
            logging.error(f"Record not found: {record_id}")
            return

        if record.path:
            logging.info(f"Skipping record {record_id}: File already downloaded")
            return

        if not url_dnb_archive:
            logging.warning(f"No URN found for record {record_id}")
            return

        result = download_and_save_file(record.idn, url_dnb_archive, download_dir, timeout=60)
        if not result:
            return

        file_name, file_size, content_type, file_extension, num_pages = result
        record.path = file_name
        record.file_size = file_size
        record.content_type = content_type
        record.file_extension = file_extension
        record.num_pages = num_pages
        retries = 3
        for attempt in range(retries):
            try:
                session.commit()
                logging.info(f"Successfully downloaded and updated record {record_id}")
                break
            except SQLAlchemyError as e:
                if 'database is locked' in str(e).lower():
                    if attempt < retries - 1:
                        logging.warning(f"Database is locked, retrying commit for record {record_id} (Attempt {attempt + 1})")
                        time.sleep(1)
                    else:
                        logging.error(f"Failed to commit record {record_id} after {retries} attempts due to database lock.")
                else:
                    logging.error(f"An error occurred while committing record {record_id}: {e}")
                    break
    except Exception as e:
        logging.error(f"An error occurred while downloading file and writing to DB for record {record_id}: {e}")
    finally:
        session.close()

def process_records(download_dir, max_concurrent_downloads=10, batch_size=1000):
    start_time = datetime.now()
    try:
        session = SessionFactory()
        records_with_url = session.query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None)
        ).scalar()

        records_with_file = session.query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None),
            DNBRecord.path.isnot(None)
        ).scalar()

        records_to_process = session.query(func.count(DNBRecord.id)).filter(
            DNBRecord.url_dnb_archive.isnot(None),
            DNBRecord.path.is_(None)
        ).scalar()

        print(f"Not processing {records_with_file} records with file already downloaded...")
        print(f"Processing {records_to_process} records...")

        completed_records = records_with_file
        session_completed = 0
        last_progress_update = datetime.now()

        with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
            active_futures = deque()
            records_generator = get_records(SessionFactory, batch_size)

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
                    session_completed += 1

                    # Update progress every 1 second
                    current_time = datetime.now()
                    if current_time - last_progress_update >= timedelta(seconds=1):
                        progress = (completed_records / records_with_url) * 100

                        # Calculate ETA
                        elapsed_time = (current_time - start_time).total_seconds()
                        records_left = records_with_url - completed_records
                        if session_completed > 0:
                            avg_time_per_record = elapsed_time / session_completed
                            eta_seconds = avg_time_per_record * records_left
                            eta = timedelta(seconds=int(eta_seconds))
                        else:
                            eta = "Unknown"

                        print_progress(f"Progress: {progress:.2f}% ({completed_records}/{records_with_url}) ETA: {eta}", end='')
                        last_progress_update = current_time

                        # Explicitly call garbage collection periodically
                        if completed_records % batch_size == 0:
                            gc.collect()

                except Exception as e:
                    logging.error(f"Exception in worker thread: {e}")

                del completed_future

        print_progress(f"\rFinished: ({records_to_process}/{records_to_process})")

    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
    finally:
        SessionFactory.remove()
        gc.collect()

    total_time = datetime.now() - start_time
    print_progress(f"Finished processing. Total time: {pretty_print_time(total_time)}")

def get_records(SessionFactory, batch_size):
    """Generator function to yield records in batches."""
    offset = 0
    while True:
        session = SessionFactory()
        try:
            records = session.query(DNBRecord).filter(
                DNBRecord.url_dnb_archive.isnot(None),
                DNBRecord.path.is_(None)
            ).order_by(DNBRecord.year.desc()).offset(offset).limit(batch_size).all()
        
            if not records:
                break
        
            for record in records:
                yield record
                
        
            offset += batch_size
        finally:
            session.close()
        gc.collect()

def print_progress(message, end='\n'):
    print(f"\r{message}", end=end, file=sys.stderr, flush=True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    download_dir = os.getenv('DOWNLOAD_DIR') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/files')
    os.makedirs(download_dir, exist_ok=True)
    logging.info(f"Download directory: {download_dir}")
    logging.getLogger('').setLevel(logging.WARNING)

    init_db(engine)  # Create tables if they don't exist


    process_records(download_dir=download_dir, max_concurrent_downloads=4, batch_size=64)
