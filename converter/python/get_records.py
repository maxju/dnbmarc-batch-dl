import os
import psycopg2
from psycopg2 import sql
from typing import List, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
# TODO: Replace these with your actual database credentials or set up environment variables
DB_NAME = os.environ.get('DB_NAME', 'your_database_name')
DB_USER = os.environ.get('DB_USER', 'your_database_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'your_database_password')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')

def get_db_connection():
    """
    Create and return a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise

def get_pdf_links() -> List[Tuple[str, str]]:
    """
    Retrieve PDF links and their associated IDs from the PostgreSQL database.
    
    Returns:
        A list of tuples containing (pdf_id, pdf_url)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # TODO: Replace 'your_table_name' with the actual table name in your database
            cur.execute(
                sql.SQL("SELECT id, pdf_url FROM your_table_name WHERE processed = FALSE")
            )
            results = cur.fetchall()
            return [(str(row[0]), row[1]) for row in results]
    except psycopg2.Error as e:
        logging.error(f"Error fetching PDF links from database: {e}")
        return []
    finally:
        conn.close()

def mark_record_as_processed(pdf_id: str, drive_file_id: str, drive_filename: str):
    """
    Mark a record as processed in the database after successful conversion and upload.
    
    Args:
        pdf_id: The ID of the PDF that has been processed
        drive_file_id: The Google Drive file ID of the uploaded file
        drive_filename: The filename of the uploaded file in Google Drive
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # TODO: Replace 'your_table_name' with the actual table name in your database
            cur.execute(
                sql.SQL("""
                    UPDATE your_table_name 
                    SET processed = TRUE, 
                        drive_file_id = %s, 
                        drive_filename = %s 
                    WHERE id = %s
                """),
                (drive_file_id, drive_filename, pdf_id)
            )
        conn.commit()
        logging.info(f"Marked record {pdf_id} as processed with Drive file ID: {drive_file_id}")
    except psycopg2.Error as e:
        logging.error(f"Error marking record {pdf_id} as processed: {e}")
        conn.rollback()
    finally:
        conn.close()
