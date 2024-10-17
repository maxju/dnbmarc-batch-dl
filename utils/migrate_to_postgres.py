import sqlite3
import os
from pg_model import Base, DNBRecord, Session

current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv('DATA_DIR') or os.path.join(current_dir, '../data')
db_path = os.path.join(DATA_DIR, 'dnb_records.db')

# SQLite connection
sqlite_conn = sqlite3.connect(db_path)
sqlite_cursor = sqlite_conn.cursor()

# Fetch all data from SQLite
sqlite_cursor.execute("SELECT * FROM dnb_records")
records = sqlite_cursor.fetchall()

# Insert data into PostgreSQL
session = Session()
for record in records:
    dnb_record = DNBRecord(
        idn=record[1],
        title=record[2],
        title_additional=record[3],
        title_author=record[4],
        author_person_id=record[5],
        author_person_name=record[6],
        author_person_role=record[7],
        author_institution_id=record[8],
        author_institution_name=record[9],
        publication_year=record[10],
        issn=record[11],
        keywords=record[12],
        country=record[13],
        language=record[14],
        ddc=record[15],
        type_of_material=record[16],
        university=record[17],
        year=record[18],
        urn=record[19],
        path=record[20],
        file_size=record[21],
        content_type=record[22],
        file_extension=record[23],
        num_pages=record[24],
        url_dnb_archive=record[25],
        url_resolving_system=record[26],
        url_publisher=record[27]
    )
    session.add(dnb_record)

session.commit()
session.close()

# Close connections
sqlite_conn.close()