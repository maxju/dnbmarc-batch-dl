from sqlalchemy import create_engine, Column, String, Integer, Sequence, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
import uuid
import os

Base = declarative_base()

# Define the DNBRecord class
class DNBRecord(Base):
    __tablename__ = 'dnb_records'
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

# Database setup
current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv('DATA_DIR') or os.path.join(current_dir, '../data')
db_path = os.path.join(DATA_DIR, 'dnb_records.db')
engine = create_engine(f'sqlite:///{db_path}')

Base.metadata.create_all(engine)

# Create a session factory
Session = sessionmaker(bind=engine)