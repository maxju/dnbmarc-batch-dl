import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import uuid
from sqlalchemy.orm import Session
from app import DNBRecord
from datetime import datetime

engine = create_engine('sqlite:///dnb_records.db')
download_dir = '../data/files/'

def pretty_print_time(duration):
            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            duration = f"{int(hours)}h{int(minutes)}m{int(seconds)}s"
            return duration

def download_and_save_file(urn, download_dir):
    try:
        response = requests.get(urn)
        response.raise_for_status()
        
        file_extension = os.path.splitext(urn)[1] or '.pdf'  # Default to .pdf if no extension
        file_name = f"{uuid.uuid4()}{file_extension}"
        file_path = os.path.join(download_dir, file_name)
        
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        return file_path
    except requests.RequestException as e:
        logging.error(f"Error downloading file from {urn}: {e}")
        return None
def process_records():
    with Session(engine) as session:
        start_time = datetime.now()
        stmt = select(DNBRecord).where(DNBRecord.urn.isnot(None), DNBRecord.path.is_(None))
        total_records = session.execute(select(func.count()).select_from(stmt.subquery())).scalar()
        print(f"Processing {total_records} records...")
        for n, record in enumerate(session.execute(stmt).scalars(), 1):
            time_taken = datetime.now() - start_time
            print(f" ({n}/{total_records}), time taken: {pretty_print_time(time_taken)}")
            
            if record.path:
                logging.info(f"Skipping record {record.id}: File already downloaded")
                continue
            
            if record.urn:
                file_path = download_and_save_file(record.urn, download_dir)
                if file_path:
                    record.path = file_path
                    session.commit()
                    logging.info(f"Downloaded and saved file for record {record.id}: {file_path}")
                else:
                    logging.warning(f"Failed to download file for record {record.id}")
            else:
                logging.warning(f"No URN found for record {record.id}")
        
        session.commit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    os.makedirs(download_dir, exist_ok=True)
    process_records()

