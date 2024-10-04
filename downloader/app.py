from lxml import etree
from pymarc import MARCReader
from pymarc import map_xml  # Import map_xml from pymarc
import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the Base class
Base = declarative_base()

# Define the DNBRecord class
class DNBRecord(Base):
    __tablename__ = 'dnb_records'
    id = Column(Integer, Sequence('record_id_seq'), primary_key=True)
    title = Column(String)
    author = Column(String)
    publication_year = Column(String)
    issn = Column(String)
    keywords = Column(String)  # Ensure this line is present
    country = Column(String)
    language = Column(String)
    ddc = Column(String)
    type_of_material = Column(String)
    university = Column(String)
    year = Column(String)
    urn = Column(String)

# Database setup
engine = create_engine('sqlite:///dnb_records.db')
Base.metadata.drop_all(engine)  # Drop the existing table if it exists
Base.metadata.create_all(engine)  # Create the tables

Session = sessionmaker(bind=engine)
session = Session()

def safe_extract(record, field_tag, subfield_code):
    """Safely extract a subfield from a MARC record."""
    try:
        return record[field_tag][subfield_code] if record.get_fields(field_tag) else None
    except KeyError:
        logging.warning(f"Field '{field_tag}' or subfield '{subfield_code}' not found in record.")
        return None

def process_record(record):
    global records
    try:
        records += 1

        type_of_material = safe_extract(record, '502', 'b')  # type of material
        university = safe_extract(record, '502', 'c')  # university
        year = safe_extract(record, '502', 'd')  # year
        title = safe_extract(record, '245', 'a')  # title
        author = safe_extract(record, '110', 'a')  # author
        publication_year = safe_extract(record, '264', 'c')  # publication year
        issn = safe_extract(record, '022', 'a')  # ISSN
        country = safe_extract(record, '044', 'c')  # country
        language = safe_extract(record, '041', 'a')  # language
        keywords = safe_extract(record, '650', 'a')  # keywords
        ddc = safe_extract(record, '082', 'a')  # DDC
        urn = safe_extract(record, '856', 'u')  # URN

        # Create a new record instance
        new_record = DNBRecord(
            title=title,
            author=author,
            publication_year=publication_year,
            issn=issn,
            keywords=keywords,
            country=country,
            language=language,
            ddc=ddc,
            type_of_material=type_of_material,
            university=university,
            year=year,
            urn=urn
        )

        records_to_add.append(new_record)

        logging.info(f"Processed record {records}: {title}")
        # Commit records in batches
        if len(records_to_add) >= batch_size:
            session.add_all(records_to_add)
            session.commit()
            records_to_add.clear()
            logging.info(f"Committed {batch_size} records")
            print(f"Committed {records} records")

    except Exception as e:
        logging.error(f"Error processing record: {e}")


# Streaming MARC parsing using map_xml
records_to_add = []
batch_size = 1000
records = 0
marc_file_path = '../data/dnb-all_online_hochschulschriften_frei_dnbmarc_20240327mrc.xml'  # Update to your XML file path
map_xml(process_record, marc_file_path)

# Commit any remaining records
if records_to_add:
    session.add_all(records_to_add)
    session.commit()
    logging.info(f"Committed {len(records_to_add)} records")