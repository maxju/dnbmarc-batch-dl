import subprocess
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
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
        # Process 'abstract' in a single call
        result = subprocess.run(
            [os.path.join(os.path.dirname(os.path.abspath(__file__)), 'find_term.sh'), file_path, '25'],
            capture_output=True, text=True
        )
        output = result.stdout.strip().split(',')
        if len(output) == 5:
            _, abstract_count, abstract_position = output
            return int(abstract_count), float(abstract_position)
        else:
            logger.warning(f"Unexpected output format for file {file_path}")
            logger.error(output)
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
        total_records = session.query(DNBRecord).count()
        logger.info(f"Total records to process: {total_records}")

        batch_size = 1000
        for offset in range(0, total_records, batch_size):
            batch = session.query(DNBRecord).offset(offset).limit(batch_size).all()
            logger.info(f"Processing batch of {len(batch)} records (offset: {offset})")
            process_batch(batch)

        logger.info("Processing complete.")
    finally:
        session.close()

if __name__ == "__main__":
    main()