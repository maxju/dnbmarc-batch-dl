import subprocess
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, DNBRecord, get_session

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
data_dir = os.getenv('data_dir') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
files_dir = os.path.join(data_dir, 'files')

# Get database connection
engine = get_engine()

def process_file(file_path):
    try:
        result = subprocess.run(
            [os.path.join(os.path.dirname(os.path.abspath(__file__)), 'find_term.sh'), file_path, '25'],
            capture_output=True, text=True
        )
        output = result.stdout.strip().split(',')
        if len(output) == 3:
            _, abstract_count, abstract_position = output
            # Clean and convert the values
            abstract_count = int(abstract_count.strip())
            abstract_position = float(abstract_position.strip())
            return abstract_count, abstract_position
        else:
            logger.warning(f"Unexpected output format for file {file_path}")
            logger.error(f"Output: {output}")
            return 0, 0.0
    except ValueError as ve:
        logger.error(f"Error converting values for file {file_path}: {str(ve)}")
        logger.error(f"Raw output: {result.stdout}")
        return 0, 0.0
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return 0, 0.0

def process_batch(batch):
    session = get_session(engine)
    try:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_record = {executor.submit(process_file, os.path.join(files_dir, record.path)): record for record in batch if record.path}
            for future in as_completed(future_to_record):
                record = future_to_record[future]
                try:
                    abstract_count, abstract_position = future.result()
                    record.abstract_num = abstract_count
                    record.abstract_pos = abstract_position
                    logger.debug(f"Processed record ID: {record.id}")
                except Exception as e:
                    logger.error(f"Error processing record ID {record.id}: {str(e)}")

        session.commit()
        logger.info(f"Committed changes for batch of {len(batch)} records")
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        session.rollback()
    finally:
        session.close()

def main():
    session = get_session(engine)
    try:
        # Count only records that need processing
        total_records = session.query(DNBRecord).filter(DNBRecord.abstract_num == None).count()
        print(f"Total records to process: {total_records}")

        batch_size = 1000
        processed_records = 0

        with tqdm(total=total_records, desc="Overall progress", unit="record") as pbar:
            for offset in range(0, total_records, batch_size):
                # Query only records that need processing
                batch = session.query(DNBRecord).filter(DNBRecord.abstract_num == None).offset(offset).limit(batch_size).all()
                
                if not batch:
                    break  # No more records to process

                print(f"Processing batch of {len(batch)} records (offset: {offset})")
                process_batch(batch)
                
                processed_records += len(batch)
                pbar.update(len(batch))
                
                # Log overall progress every 5% or every 1,000 records, whichever comes first
                if processed_records % max(total_records // 20, 1000) == 0 or processed_records == total_records:
                    progress_percentage = (processed_records / total_records) * 100
                    print(f"Overall progress: {processed_records}/{total_records} records processed ({progress_percentage:.1f}%)")

        logger.info("Processing complete.")
    finally:
        session.close()

if __name__ == "__main__":
    main()