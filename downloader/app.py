from lxml import etree
from pymarc import MARCReader
from pymarc import map_xml
import logging
import os
from sqlalchemy import create_engine, Column, String, Integer, Sequence, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid
from sqlalchemy.dialects.postgresql import UUID

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the Base class
Base = declarative_base()

# Define the DNBRecord class
class DNBRecord(Base):
    __tablename__ = 'dnb_records'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4) # internal db uuid
    idn = Column(String, unique=True, index=True)  # DNB IDN identifier
    title = Column(Text)
    title_additional = Column(Text)
    title_author = Column(Text)
    author_person_id = Column(String)
    author_person_name = Column(Text)
    author_person_role = Column(String)
    author_institution_id = Column(String)
    author_institution_name = Column(Text)
    publication_year = Column(String)
    issn = Column(String)
    keywords = Column(Text)
    country = Column(String)
    language = Column(String)
    ddc = Column(String)
    type_of_material = Column(String)
    university = Column(String)
    year = Column(String)
    urn = Column(String)
    path = Column(String)
    url_dnb_archive = Column(String)
    url_resolving_system = Column(String)
    url_publisher = Column(String)

# Database setup
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, 'dnb_records.db')
engine = create_engine(f'sqlite:///{db_path}')
Base.metadata.drop_all(engine)  # ACHTUNG! Drop the existing table if it exists
Base.metadata.create_all(engine)  # Create the tables

Session = sessionmaker(bind=engine)
session = Session()

def safe_extract(record, field_tag, subfield_code, ind1=None, ind2=None):
    """Safely extract a subfield from a MARC record, optionally matching indicators."""
    try:
        fields = record.get_fields(field_tag)
        if ind1 is not None and ind2 is not None:
            fields = [f for f in fields if f.indicator1 == ind1 and f.indicator2 == ind2]
        
        if fields:
            if subfield_code is None:
                return fields[0].data if fields[0].data else None
            else:
                return fields[0].get_subfields(subfield_code)[0] if fields[0].get_subfields(subfield_code) else None
        return None
    except Exception as e:
        logging.warning(f"Error extracting field '{field_tag}' subfield '{subfield_code}': {e}")
        return None

def process_record(record):
    global records
    try:
        records += 1
        identifier = safe_extract(record, '001', None) # IDN identifier
        logging.info(f"Processed record {records}: {identifier}")
        title = safe_extract(record, '245', 'a')  # title
        title_additional = safe_extract(record, '245', 'b')  # title additional
        title_author = safe_extract(record, '245', 'c')  # title author
        type_of_material = safe_extract(record, '502', 'b')  # type of material
        university = safe_extract(record, '502', 'c')  # university
        # Extract year from 008 field
        year = None
        field_008 = record.get_fields('008')
        if field_008:
            year_str = field_008[0].data[7:11]
            if year_str.isdigit():
                year = year_str
        author_person = safe_extract(record, '100', 'a')  # author person
        author_person_role = safe_extract(record, '100', 'e')  # author person role
        author_person_id = safe_extract(record, '100', '0')  # author person id
        author_institution_id = safe_extract(record, '110', '0')  # author institution id
        author_institution_name = safe_extract(record, '110', 'a')  # author institution name
        publication_year = safe_extract(record, '264', 'c')  # publication year
        issn = safe_extract(record, '022', 'a')  # ISSN
        country = safe_extract(record, '044', 'c')  # country
        language = safe_extract(record, '041', 'a')  # language
        keywords = safe_extract(record, '650', 'a')  # keywords
        ddc = safe_extract(record, '082', 'a')  # DDC
        urn = safe_extract(record, '856', 'u')  # URN
        url_resolving_system = safe_extract(record, '856', 'u', ind1='4', ind2='0')  # Resolving System URL
        url_dnb_archive = safe_extract(record, '856', 'u', ind1=' ', ind2='0')  # DNB Archive URL (Langzeitarchivierung)
        url_publisher = safe_extract(record, '856', 'u', ind1='4', ind2=' ')  # Publisher URL

        # Create a new record instance
        new_record = DNBRecord(
            idn=identifier,
            title=title,
            title_additional=title_additional,
            title_author=title_author,
            author_person_id=author_person_id,
            author_person_name=author_person,
            author_person_role=author_person_role,
            author_institution_id=author_institution_id,
            author_institution_name=author_institution_name,
            publication_year=publication_year,
            issn=issn,
            keywords=keywords,
            country=country,
            language=language,
            ddc=ddc,
            type_of_material=type_of_material,
            university=university,
            year=year,
            urn=urn,
            url_dnb_archive=url_dnb_archive,
            url_resolving_system=url_resolving_system,
            url_publisher=url_publisher
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