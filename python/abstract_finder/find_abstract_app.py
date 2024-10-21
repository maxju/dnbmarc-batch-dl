import subprocess
import os
import sys
import logging
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, DNBRecord

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
data_dir = os.getenv('data_dir') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
files_dir = os.path.join(data_dir, 'files')

# Get database connection
engine = get_engine()
Session = sessionmaker(bind=engine)

def process_batch(batch):
    session = Session()
    try:
        for record in batch:
            if record.path:
                file_path = os.path.join(files_dir, record.path)
                logger.info(f"Processing file: {file_path}")

                # Process 'abstract'
                result = subprocess.run([os.path.join(os.path.dirname(os.path.abspath(__file__)), 'find_term.sh'), record.path, 'abstract'], 
                                        capture_output=True, text=True)
                output = result.stdout.strip().split(',')
                if len(output) == 3:
                    filename, abstract_count, abstract_position = output
                    record.abstract_num = int(abstract_count)
                    record.abstract_pos = float(abstract_position)
                    logger.info(f"Updated abstract info: count={abstract_count}, position={abstract_position}")
                else:
                    logger.warning(f"Unexpected output format for 'abstract' in file {record.path}")

                # Process 'summary'
                # Start of Selection
                result = subprocess.run([os.path.join(os.path.dirname(os.path.abspath(__file__)), 'find_term.sh'), record.path, 'summary'], 
                                        capture_output=True, text=True)
                output = result.stdout.strip().split(',')
                if len(output) == 3:
                    filename, summary_count, summary_position = output
                    record.summary_num = int(summary_count)
                    record.summary_pos = float(summary_position)
                    logger.info(f"Updated summary info: count={summary_count}, position={summary_position}")
                else:
                    logger.warning(f"Unexpected output format for 'summary' in file {record.path}")
            else:
                logger.error(f"No path found for record with ID: {record.id}")

        session.commit()
        logger.info(f"Committed changes for batch of {len(batch)} records")
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        session.rollback()
    finally:
        session.close()

def main():
    session = Session()
    try:
        total_records = session.query(DNBRecord).count()
        logger.info(f"Total records to process: {total_records}")

        batch_size = 100
        for offset in range(0, total_records, batch_size):
            batch = session.query(DNBRecord).offset(offset).limit(batch_size).all()
            logger.info(f"Processing batch of {len(batch)} records (offset: {offset})")
            process_batch(batch)

        logger.info("Processing complete.")
    finally:
        session.close()

if __name__ == "__main__":
    main()