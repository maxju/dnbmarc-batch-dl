import subprocess
import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.pg_model import get_engine, get_session, DNBRecord

# Load environment variables
load_dotenv()
data_dir = os.getenv('data_dir') or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
files_dir = os.path.join(data_dir, 'files')

# Get database connection
engine = get_engine()
session = get_session(engine)

# Query all records from the database
records = session.query(DNBRecord).all()

for record in records:
    if record.path:
        file_path = os.path.join(files_dir, record.path)
        # Run the bash script for "abstract" and capture its output
        result = subprocess.run(['./utils/find_abstract.sh', record.path], 
                                capture_output=True, text=True)
        
        # Parse the output
        output = result.stdout.strip().split(',')
        if len(output) == 3:
            filename, abstract_count, abstract_position = output
            
            # Update the record
            record.abstract_num = int(abstract_count)
            record.abstract_pos = float(abstract_position)

        # Run the script again for "summary"
        result = subprocess.run(['./utils/find_term.sh', record.path, 'summary'], 
                                capture_output=True, text=True)
        
        output = result.stdout.strip().split(',')
        if len(output) == 3:
            filename, summary_count, summary_position = output
            
            record.summary_num = int(summary_count)
            record.summary_pos = float(summary_position)
    else:
        print(f"No path found for record with ID: {record.id}")

    # Commit the changes after processing each record
    session.commit()

# Close the session
session.close()

print("Processing complete.")
