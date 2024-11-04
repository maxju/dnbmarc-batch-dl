from sqlalchemy import create_engine, Column, String, Integer, Text, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os
from dotenv import load_dotenv

# Load environment variables from the project root
load_dotenv()
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

Base = declarative_base()

class DNBRecord(Base):
    __tablename__ = "dnb_records_subset"
    id = Column(Integer, primary_key=True, autoincrement=True)
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
    file_size = Column(Integer)
    content_type = Column(String)
    file_extension = Column(String)
    num_pages = Column(Integer)
    url_dnb_archive = Column(String)
    url_resolving_system = Column(String)
    url_publisher = Column(String)
    selection = Column(Integer, default=0)
    converted_file = Column(String)
    drive_file_id = Column(String)
    conversion_lock = Column(Date)
    abstract_num = Column(Integer)
    summary_num = Column(Integer)
    abstract_pos = Column(Integer)
    summary_pos = Column(Integer)

def get_engine(database_url=None):
    if database_url is None:
        PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
        PG_PASS = os.getenv('POSTGRES_PASSWORD')
        PG_PORT = os.getenv('POSTGRES_PORT', 5432)
        DATABASE_URL = f"postgresql://dnb:{PG_PASS}@{PG_HOST}:{PG_PORT}/dnb_records"
    else:
        DATABASE_URL = database_url
    return create_engine(DATABASE_URL)

def init_db(engine):
    Base.metadata.create_all(engine)

def get_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

# Only create tables if this file is run directly
if __name__ == "__main__":
    engine = get_engine()
    print(f"Creating tables in PostgreSQL database")
    init_db(engine)
