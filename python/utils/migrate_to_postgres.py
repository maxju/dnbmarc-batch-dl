import sqlite3
import os
import sys
from pg_model import get_engine, get_session, DNBRecord, init_db
from sqlalchemy import func
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv('data_dir') or os.path.join(current_dir, '../data')
db_path = os.path.join(DATA_DIR, 'dnb_records.db')

# SQLite connection
sqlite_conn = sqlite3.connect(db_path)
sqlite_cursor = sqlite_conn.cursor()

def safe_int(value):
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None

# Fetch all data from SQLite
sqlite_cursor.execute("SELECT * FROM dnb_records")
column_names = [description[0] for description in sqlite_cursor.description]
records = [dict(zip(column_names, record)) for record in sqlite_cursor.fetchall()]

# Insert data into PostgreSQL
engine = get_engine()
session = get_session(engine)

try:
    is_empty = session.query(func.count(DNBRecord.id)).scalar() == 0
except ProgrammingError:
    # Table doesn't exist, so we'll consider it empty
    is_empty = True
    init_db(engine)

if is_empty:
    successful_inserts = 0
    failed_inserts = 0
    for record in records:
        dnb_record = DNBRecord(
            idn=record['idn'],
            title=record['title'],
            title_additional=record['title_additional'],
            title_author=record['title_author'],
            author_person_id=record['author_person_id'],
            author_person_name=record['author_person_name'],
            author_person_role=record['author_person_role'],
            author_institution_id=record['author_institution_id'],
            author_institution_name=record['author_institution_name'],
            publication_year=record['publication_year'],
            issn=record['issn'],
            keywords=record['keywords'],
            country=record['country'],
            language=record['language'],
            ddc=record['ddc'],
            type_of_material=record['type_of_material'],
            university=record['university'],
            year=record['year'],
            urn=record['urn'],
            path=record['path'],
            file_size=record['file_size'],
            content_type=record['content_type'],
            file_extension=record['file_extension'],
            num_pages=record['num_pages'],
            url_dnb_archive=record['url_dnb_archive'],
            url_resolving_system=record['url_resolving_system'],
            url_publisher=record['url_publisher']
        )
        try:
            session.add(dnb_record)
            session.flush()  # This will attempt to insert the record without committing
            successful_inserts += 1
        except SQLAlchemyError as e:
            print(f"Error inserting record with idn {record['idn']}:")
            print(str(e))
            print("Problematic record data:")
            print(json.dumps({k: str(v) for k, v in dnb_record.__dict__.items() if not k.startswith('_')}, indent=2))
            session.rollback()  # Roll back the failed insertion
            failed_inserts += 1
            continue  # Skip to the next record

    try:
        session.commit()
        print(f"Successfully migrated {successful_inserts} records to PostgreSQL.")
        print(f"Failed to insert {failed_inserts} records.")
    except SQLAlchemyError as e:
        print("Error committing the transaction:")
        print(str(e))
        session.rollback()
else:
    print("PostgreSQL table is not empty. Skipping migration to avoid duplicates.")

session.close()

# Close connections
sqlite_conn.close()
