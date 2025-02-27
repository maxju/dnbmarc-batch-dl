import logging
import os
import sys
from sqlalchemy import func
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from typing import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
import tempfile
import shutil
import fitz  # PyMuPDF
import gc
from tqdm import tqdm
import concurrent.futures

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, DNBRecord

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace existing session management code
engine = get_engine()
SessionFactory = scoped_session(sessionmaker(bind=engine))

def get_session():
    return SessionFactory()

def get_records_generator(batch_size: int = 100) -> Generator[DNBRecord, None, None]:
    """Generator that yields records needing download"""
    offset = 0
    while True:
        session = SessionFactory()
        try:
            results = session.query(DNBRecord).filter(
                DNBRecord.url_dnb_archive.isnot(None),
                DNBRecord.converted_file.is_(None)  # Only unconverted records
            ).order_by(DNBRecord.idn).offset(offset).limit(batch_size).all()
            
            if not results:
                break
                
            for record in results:
                yield record
                
            offset += batch_size
        finally:
            session.close()
            SessionFactory.remove()
            time.sleep(0.1)  # Brief pause between batches

def download_and_save_file(id, url, download_dir):
    """Downloads PDF without updating database"""
    temp_file_path = None
    try:
        # Existing download logic from downloader.py
        with requests.get(url, timeout=60, allow_redirects=True, stream=True) as response:
            response.raise_for_status()

            file_extension = os.path.splitext(url)[1] or '.pdf'
            file_name = f"{id}{file_extension}"
            final_file_path = os.path.join(download_dir, file_name)

            # Skip if file already exists
            if os.path.exists(final_file_path):
                logging.info(f"File already exists: {file_name}")
                return file_name

            # Temp file handling
            with tempfile.NamedTemporaryFile(delete=False, dir=download_dir, 
                                           prefix=f"{id}_temp_", suffix=file_extension) as temp_file:
                temp_file_path = temp_file.name
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)

            # Atomic move
            shutil.move(temp_file_path, final_file_path)
            logging.info(f"Downloaded {file_name} to {download_dir}")

            # Get PDF metadata
            num_pages = get_pdf_pages(final_file_path)
            file_size = os.path.getsize(final_file_path)
            
            # Return values but don't write to DB
            return {
                'file_name': file_name,
                'file_size': file_size,
                'num_pages': num_pages
            }

    except Exception as e:
        logging.error(f"Download failed for {id}: {str(e)}")
        return None
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logging.error(f"Temp file cleanup failed: {str(e)}")

def download_and_update(record: DNBRecord, download_dir: str):
    """Modified version without DB writes"""
    try:
        with get_session() as session:
            # Corrected line: filter by idn instead of using get() with primary key
            fresh_record = session.query(DNBRecord).filter(DNBRecord.idn == record.idn).first()
            if not fresh_record:
                return

            # Skip if file exists locally
            expected_path = os.path.join(download_dir, f"{fresh_record.idn}.pdf")
            if os.path.exists(expected_path):
                return

            # Perform download
            result = download_and_save_file(
                fresh_record.idn,
                fresh_record.url_dnb_archive,
                download_dir
            )

            if result:
                fresh_record.path = result['file_name']
                fresh_record.file_size = result['file_size']
                fresh_record.num_pages = result['num_pages']
                session.commit()
                logging.info(f"Downloaded and updated in DB: {fresh_record.idn}")
                
    except Exception as e:
        logging.error(f"Error processing {record.idn}: {str(e)}")

def process_downloads(download_dir: str, max_workers: int = 4):
    """Main processing loop with proper progress tracking"""
    from tqdm import tqdm

    try:
        with get_session() as session:
            total_to_download = session.query(func.count(DNBRecord.idn)).filter(
                DNBRecord.url_dnb_archive.isnot(None),
                DNBRecord.converted_file.is_(None)
            ).scalar()
            
        if not total_to_download:
            logging.info("No records to download")
            return

        logging.info(f"Found {total_to_download:,} unconverted records to download")

        # Use tqdm for progress tracking
        with tqdm(total=total_to_download, desc="Downloading PDFs", unit="file") as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                for record in get_records_generator():
                    future = executor.submit(download_and_update, record, download_dir)
                    futures.append(future)
                    
                    # Update progress
                    while len(futures) >= max_workers * 2:
                        done, _ = concurrent.futures.wait(futures, timeout=0.1)
                        for f in done:
                            try:
                                f.result()
                                pbar.update(1)
                            except Exception as e:
                                logging.error(f"Error in future: {str(e)}")
                            futures.remove(f)

                # Process remaining futures
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                        pbar.update(1)
                    except Exception as e:
                        logging.error(f"Error in future: {str(e)}")

    except KeyboardInterrupt:
        logging.info("Download process interrupted by user")
    finally:
        logging.info("Download process completed")

def get_pdf_pages(file_path):
    """Direct copy from downloader/downloader.py"""
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

if __name__ == "__main__":
    download_dir = os.getenv('DOWNLOAD_DIR', os.path.expanduser('data/pdf'))
    os.makedirs(download_dir, exist_ok=True)
    
    logging.info(f"Starting download of unconverted records to {download_dir}")
    process_downloads(download_dir)
    logging.info("Download process completed successfully")
