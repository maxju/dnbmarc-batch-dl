import logging
from typing import List, Tuple, Generator, Set
from sqlalchemy import func
import sys
import os
from datetime import timedelta
import time
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, get_session, DNBRecord, retry_on_db_error

engine = get_engine()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProgressTracker:
    def __init__(self, total_records: int, batch_size: int):
        self.total_records = total_records
        self.batch_size = batch_size
        self.processed_records = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.update_interval = 20  # seconds between progress updates

    def update(self, batch_count: int) -> None:
        self.processed_records += len(batch_count)
        current_time = time.time()
        
        # Only update if enough time has passed
        if current_time - self.last_update_time >= self.update_interval:
            self.print_progress()
            self.last_update_time = current_time

    def print_progress(self) -> None:
        elapsed_time = time.time() - self.start_time
        progress = (self.processed_records / self.total_records) * 100
        
        # Calculate speed and remaining time
        records_per_second = self.processed_records / elapsed_time
        remaining_records = self.total_records - self.processed_records
        estimated_remaining_seconds = remaining_records / records_per_second if records_per_second > 0 else 0
        
        # Format times
        elapsed = str(timedelta(seconds=int(elapsed_time)))
        remaining = str(timedelta(seconds=int(estimated_remaining_seconds)))
        records_per_hour = records_per_second * 3600
        
        logging.info(
            f"Progress: {progress:.1f}% ({self.processed_records}/{self.total_records}) | "
            f"Speed: {records_per_hour:.1f} records/hour | "
            f"Elapsed: {elapsed} | "
            f"Remaining: {remaining}"
        )

@retry_on_db_error()
def get_total_records() -> int:
    """Get total number of unprocessed records."""
    with get_session(engine) as session:
        return session.query(func.count(DNBRecord.idn))\
            .filter(DNBRecord.url_dnb_archive.isnot(None))\
            .filter(DNBRecord.converted_file.is_(None))\
            .scalar()

@retry_on_db_error()
def mark_record_as_processed(pdf_id: str, drive_file_id: str, drive_filename: str):
    """Mark a record as processed in the database."""
    with get_session(engine) as session:
        record = session.query(DNBRecord).filter(DNBRecord.idn == pdf_id).first()
        if record:
            record.converted_file = drive_filename
            record.drive_file_id = drive_file_id
            session.commit()
            logging.info(f"Marked record {pdf_id} as processed with Drive file ID: {drive_file_id}")
        else:
            logging.warning(f"Record with ID {pdf_id} not found")

def load_blacklist() -> Set[str]:
    """Load the blacklist of failed PDF IDs from file."""
    blacklist_file = "failed_pdfs.json"
    if os.path.exists(blacklist_file):
        try:
            with open(blacklist_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")
    return set()

def get_pdf_links(batch_size: int = 100) -> Generator[List[Tuple[str, str]], None, None]:
    """Retrieve PDF links with retry logic."""
    try:
        total_records = get_total_records()
        progress = ProgressTracker(total_records, batch_size)
        blacklist = load_blacklist()  # Load blacklist once at start
        
        offset = 0
        while True:
            try:
                with get_session(engine) as session:
                    # Query that excludes blacklisted IDs
                    results = session.query(DNBRecord.idn, DNBRecord.url_dnb_archive)\
                        .filter(DNBRecord.url_dnb_archive.isnot(None))\
                        .filter(DNBRecord.converted_file.is_(None))\
                        .filter(~DNBRecord.idn.in_(blacklist))\
                        .order_by(DNBRecord.idn)\
                        .offset(offset)\
                        .limit(batch_size)\
                        .all()

                if not results:
                    break
                
                batch = [(str(row.idn), row.url_dnb_archive) for row in results]
                progress.update(batch)
                yield batch
                offset += batch_size
                
            except (OperationalError, SQLAlchemyError) as e:
                logging.error(f"Database error while fetching batch at offset {offset}: {e}")
                # Wait before retrying this batch
                time.sleep(5)
                continue
                
    except Exception as e:
        logging.error(f"Error in get_pdf_links: {e}")
