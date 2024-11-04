import logging
from typing import List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, init_db, get_session, DNBRecord

engine = get_engine()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pdf_links(batch_size: int = 100) -> List[Tuple[str, str]]:
    """
    Retrieve PDF links and their associated IDs from the PostgreSQL database in batches.
    
    Args:
        batch_size: Number of records to fetch in each batch

    Yields:
        A list of tuples containing (pdf_id, pdf_url)
    """
    try:
        with get_session(engine) as session:
            offset = 0
            while True:
                results = session.query(DNBRecord.idn, DNBRecord.url_dnb_archive)\
                    .filter(DNBRecord.url_dnb_archive.isnot(None))\
                    .filter(DNBRecord.converted_file.is_(None))\
                    .order_by(DNBRecord.idn)\
                    .offset(offset)\
                    .limit(batch_size)\
                    .all()

                if not results:
                    break
                
                yield [(str(row.idn), row.url_dnb_archive) for row in results]
                offset += batch_size
    except Exception as e:
        logging.error(f"Error fetching PDF links from database: {e}")

def mark_record_as_processed(pdf_id: str, drive_file_id: str, drive_filename: str):
    """
    Mark a record as processed in the database after successful conversion and upload.
    
    Args:
        pdf_id: The ID of the PDF that has been processed
        drive_file_id: The Google Drive file ID of the uploaded file
        drive_filename: The filename of the uploaded file in Google Drive
    """
    try:
        with get_session(engine) as session:
            record = session.query(DNBRecord).filter(DNBRecord.id == pdf_id).first()
            if record:
                record.converted_file = drive_filename
                record.drive_file_id = drive_file_id
                session.commit()
                logging.info(f"Marked record {pdf_id} as processed with Drive file ID: {drive_file_id}")
            else:
                logging.warning(f"Record with ID {pdf_id} not found")
    except Exception as e:
        logging.error(f"Error marking record {pdf_id} as processed: {e}")
