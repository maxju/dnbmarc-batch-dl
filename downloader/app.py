from lxml import etree
from pymarc import MARCReader
from pymarc import map_xml
import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the Base class
Base = declarative_base()

# Define the DNBRecord class
class DNBRecord(Base):
    __tablename__ = 'dnb_records'
    id = Column(Integer, primary_key=True)
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
    path = Column(String) # file system path to pdf

# Database setup
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, 'dnb_records.db')
engine = create_engine(f'sqlite:///{db_path}')
Base.metadata.drop_all(engine)  # ACHTUNG! Drop the existing table if it exists
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
        identifier = safe_extract(record, '001', 'a')
        logging.info(f"Processed record {records}: {identifier}")
        title = safe_extract(record, '245', 'a')  # title
        type_of_material = safe_extract(record, '502', 'b')  # type of material
        university = safe_extract(record, '502', 'c')  # university
        year = safe_extract(record, '502', 'd')  # year
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
            id=identifier,
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

        # Commit records in batches
        if len(records_to_add) >= batch_size:
            session.add_all(records_to_add)
            session.commit()
            records_to_add.clear()
            logging.info(f"Committed {batch_size} records")
            print(f"Committed {records} records")

    except Exception as e:
        logging.error(f"Error processing record: {e}")


# Initialize variables
records_to_add = []
batch_size = 1000
records = 0

# Construct the path to the MARC XML file
data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
marc_file = 'dnb-all_online_hochschulschriften_frei_dnbmarc_20240327mrc.xml'
marc_file_path = os.path.join(data_dir, marc_file)

# Process the MARC XML file
map_xml(process_record, marc_file_path)

# Commit any remaining records
if records_to_add:
    session.add_all(records_to_add)
    session.commit()
    logging.info(f"Committed {len(records_to_add)} records")