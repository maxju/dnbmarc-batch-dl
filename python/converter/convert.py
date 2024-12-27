import os
import logging
import sys
from collections import deque
import requests
from typing import Optional, Tuple, List, Set
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from pathlib import Path
from dotenv import load_dotenv
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from marker.config.parser import ConfigParser
from marker.settings import settings
from tqdm import tqdm
import shutil
import PyPDF2
import json
from functools import partial

os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"  # For M1/M2 Macs
os.environ["EXTRACT_IMAGES"] = "false"  # Disable image extraction
os.environ["IN_STREAMLIT"] = "true" # Avoid multiprocessing inside surya
os.environ["PDFTEXT_CPU_WORKERS"] = "1" # Avoid multiprocessing inside pdftext


# Constants
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
LOCAL_TEMP_DIR = "temp_files"
BATCH_SIZE = 5
BLACKLIST_FILE = "failed_pdfs.json"

def load_blacklist() -> dict:
    """
    Load the blacklist of failed PDF IDs and their reasons from file
    """
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")
    return {}

def add_to_blacklist(pdf_id: str, reason: str):
    """
    Add a PDF ID to the blacklist with a reason
    """
    blacklist = load_blacklist()
    blacklist[pdf_id] = reason
    try:
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(blacklist, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving blacklist: {e}")

def validate_pdf(file_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate if the downloaded file is a valid PDF.
    Performs multiple checks to ensure the PDF is not corrupt and within size limits.
    Returns (is_valid, reason_if_invalid)
    """
    try:
        with open(file_path, 'rb') as file:
            # Check if file starts with PDF signature
            if not file.read(5).startswith(b'%PDF-'):
                return False, "Invalid PDF signature"
                
            # Reset file pointer
            file.seek(0)
            
            # Try to read PDF structure
            reader = PyPDF2.PdfReader(file)
            
            # Check if PDF has pages
            if len(reader.pages) == 0:
                return False, "PDF has no pages"

            # Check if PDF exceeds page limit
            if len(reader.pages) > 200:
                return False, f"PDF exceeds 200 page limit (has {len(reader.pages)} pages)"
                
            # Try to access first page to verify basic structure
            try:
                _ = reader.pages[0]
            except Exception as e:
                return False, f"Cannot access first page: {str(e)}"
                
        return True, None
    except Exception as e:
        return False, f"PDF validation failed: {str(e)}"

def download_with_retry(pdf_id: str, pdf_url: str, local_path: str, max_retries: int = 3) -> bool:
    """
    Download a PDF with retry mechanism.
    Only retry on network/download errors, not on validation failures.
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Add detailed logging
            logging.info(f"Downloading PDF {pdf_id} (attempt {attempt + 1}/{max_retries})")
            logging.info(f"URL: {pdf_url}")
            logging.info(f"Local path: {local_path}")
            
            # Download the file
            response = requests.get(pdf_url, stream=True)
            response.raise_for_status()
            
            # Log response details
            logging.info(f"Response status: {response.status_code}")
            logging.info(f"Content type: {response.headers.get('content-type', 'unknown')}")
            
            # Write to temporary file first
            temp_path = f"{local_path}.tmp"
            with open(temp_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            
            # Validate the downloaded file
            is_valid, reason = validate_pdf(temp_path)
            if is_valid:
                # If valid, move to final location
                os.rename(temp_path, local_path)
                return True
            else:
                # If invalid, cleanup and blacklist
                os.remove(temp_path)
                add_to_blacklist(pdf_id, reason)
                logging.warning(f"Downloaded file is not valid: {reason}")
                # Return False immediately on validation failure - don't retry
                return False
                
        except requests.RequestException as e:
            # Handle download/network errors
            last_error = f"Download failed: {str(e)}"
            logging.warning(f"Download attempt {attempt + 1}/{max_retries} failed for PDF {pdf_id}: {last_error}")
            
            # Cleanup any partial downloads
            if os.path.exists(f"{local_path}.tmp"):
                os.remove(f"{local_path}.tmp")
            if os.path.exists(local_path):
                os.remove(local_path)
                
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            continue  # Try next attempt
                
        except Exception as e:
            # Handle any other unexpected errors
            last_error = f"Unexpected error: {str(e)}"
            logging.error(f"Unexpected error processing PDF {pdf_id}: {last_error}")
            
            # Cleanup any partial downloads
            if os.path.exists(f"{local_path}.tmp"):
                os.remove(f"{local_path}.tmp")
            if os.path.exists(local_path):
                os.remove(local_path)
            break  # Don't retry on unexpected errors
    
    # If we get here, all retries failed or there was an unexpected error
    add_to_blacklist(pdf_id, last_error or "Unknown error")
    return False

def download_batch(batch: List[Tuple[str, str]], temp_dir: str) -> List[Tuple[str, str, str]]:
    """
    Download a batch of PDFs and return list of (pdf_id, pdf_url, local_path)
    """
    downloaded = []
    blacklist = load_blacklist()  # Load blacklist at start of batch
    
    with tqdm(total=len(batch), desc="Downloading PDFs", unit="pdf") as pbar:
        for pdf_id, pdf_url in batch:
            try:
                # Skip already blacklisted PDFs
                if pdf_id in blacklist:
                    logging.info(f"Skipping blacklisted PDF {pdf_id}: {blacklist[pdf_id]}")
                    mark_record_as_processed(pdf_id, None, f"Blacklisted: {blacklist[pdf_id]}")
                    pbar.update(1)
                    continue
                    
                local_path = os.path.join(temp_dir, f"{pdf_id}.pdf")
                if download_with_retry(pdf_id, pdf_url, local_path):
                    downloaded.append((pdf_id, pdf_url, local_path))
                else:
                    # Mark failed downloads as processed to avoid retrying them
                    failure_reason = blacklist.get(pdf_id, "Unknown error")
                    mark_record_as_processed(pdf_id, None, f"Failed: {failure_reason}")
            except Exception as e:
                logging.error(f"Error processing PDF {pdf_id}: {str(e)}")
                mark_record_as_processed(pdf_id, None, f"Error: {str(e)}")
            finally:
                pbar.update(1)
                
    return downloaded

def upload_file_to_drive(service, file_path: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Upload a file to Google Drive and delete it locally if successful.
    
    Args:
        service: Authenticated Google Drive service object
        file_path: Local path of the file to upload
    
    Returns:
        Tuple of (success: bool, file_id: Optional[str], filename: Optional[str])
    """
    try:
        file_id, filename = dfm.upload_file(service, file_path, DRIVE_FOLDER_ID)
        if file_id and filename:
            uploaded_folder = os.path.join(os.path.dirname(file_path), 'uploaded')
            os.makedirs(uploaded_folder, exist_ok=True)
            new_path = os.path.join(uploaded_folder, os.path.basename(file_path))
            os.rename(file_path, new_path)
            logging.info(f"Successfully uploaded and moved to {new_path}")
            return True, file_id, filename
        else:
            logging.error(f"Failed to upload {file_path}: file_id or filename is None")
            return False, None, None
    except Exception as e:
        logging.error(f"Failed to upload {file_path}: {str(e)}")
        return False, None, None

def process_batch(drive_service, downloaded_batch: List[Tuple[str, str, str]], converter: PdfConverter) -> List[bool]:
    """
    Process a batch of downloaded PDFs individually
    """
    results = []
    
    with tqdm(total=len(downloaded_batch), desc="Converting PDFs", unit="pdf") as pbar:
        for pdf_id, pdf_url, pdf_path in downloaded_batch:
            mmd_path = pdf_path.replace('.pdf', '.mmd')  # Initialize mmd_path outside try block
            try:
                # Add detailed logging and validation
                logging.info(f"Processing PDF {pdf_id}")
                logging.info(f"PDF path: {pdf_path}")
                
                if not os.path.exists(pdf_path):
                    logging.error(f"PDF file does not exist: {pdf_path}")
                    results.append(False)
                    continue
                    
                file_size = os.path.getsize(pdf_path)
                logging.info(f"File size: {file_size}")
                
                if file_size == 0:
                    logging.error(f"PDF file is empty: {pdf_path}")
                    results.append(False)
                    continue
                
                # Validate PDF structure again before processing
                is_valid, reason = validate_pdf(pdf_path)
                if not is_valid:
                    logging.error(f"PDF validation failed before processing: {reason}")
                    results.append(False)
                    continue
                
                # Log PDF metadata
                try:
                    with open(pdf_path, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        logging.info(f"PDF pages: {len(reader.pages)}")
                        logging.info(f"PDF metadata: {reader.metadata}")
                except Exception as e:
                    logging.error(f"Error reading PDF metadata: {str(e)}")
                    results.append(False)
                    continue
                
                # Convert individual PDF
                rendered = converter(pdf_path)
                markdown_text, _, _ = text_from_rendered(rendered)
                
                # Save markdown
                with open(mmd_path, 'w', encoding='utf-8') as f:
                    f.write(markdown_text)
                
                # Upload to drive
                success, file_id, filename = upload_file_to_drive(drive_service, mmd_path)
                if success:
                    mark_record_as_processed(pdf_id, file_id, filename)
                    results.append(True)
                else:
                    results.append(False)
                    
            except Exception as e:
                logging.error(f"Error processing PDF {pdf_id}: {str(e)}")
                results.append(False)
            finally:
                # Cleanup
                try:
                    if os.path.exists(pdf_path):
                        os.remove(pdf_path)
                    if os.path.exists(mmd_path):
                        os.remove(mmd_path)
                except Exception as cleanup_error:
                    logging.error(f"Error during cleanup for PDF {pdf_id}: {str(cleanup_error)}")
            pbar.update(1)
            
    return results

def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Add heartbeat file management
    heartbeat_file = "converter_heartbeat.txt"
    def update_heartbeat():
        with open(heartbeat_file, 'w') as f:
            f.write(str(time.time()))

    try:
        logging.info(f"Using batch size {BATCH_SIZE}")
        # Configure tqdm to write to stderr
        tqdm.write = partial(print, file=sys.stderr)
        drive_service = dfm.get_drive_service()
        
        # Create batch-specific temp directories
        batch_temp_dir = os.path.join(LOCAL_TEMP_DIR, "batch_processing")
        os.makedirs(batch_temp_dir, exist_ok=True)

        batch_count = 0
        last_successful_conversion = time.time()
        
        # Create converter instance with German language configuration
        config = {
            "output_format": "markdown",
            "TORCH_DEVICE": settings.TORCH_DEVICE,
            "EXTRACT_IMAGES": "false"
        }
        config_parser = ConfigParser(config)
        
        converter = PdfConverter(
            config=config_parser.generate_config_dict(),
            artifact_dict=create_model_dict(),
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer()
        )
        
        # Process PDFs in batches
        for batch in get_pdf_links(BATCH_SIZE):
            update_heartbeat()
            
            if not batch:
                logging.info("No PDFs to process in batch. Exiting.")
                sys.exit(0)
                
            logging.info(f"Processing batch number {batch_count} containing {len(batch)} PDFs")
            
            # Create a new directory for this batch
            current_batch_dir = os.path.join(batch_temp_dir, f"batch_{batch_count}")
            os.makedirs(current_batch_dir, exist_ok=True)
            
            # Download all PDFs in batch
            downloaded_batch = download_batch(batch, current_batch_dir)
            
            if not downloaded_batch:
                logging.warning("No PDFs were successfully downloaded in this batch")
                # Mark failed downloads as processed to avoid infinite loops
                for pdf_id, _ in batch:
                    if pdf_id not in blacklist:
                        mark_record_as_processed(pdf_id, None, f"Download-Failed-{pdf_id}")
                continue
            
            # Process each PDF in the batch
            results = process_batch(drive_service, downloaded_batch, converter)
            
            processed_count = sum(1 for result in results if result)
            
            if processed_count > 0:
                last_successful_conversion = time.time()
            elif time.time() - last_successful_conversion > 900:  # 15 minutes
                logging.error("No successful conversions in 15 minutes. Exiting.")
                sys.exit(1)
            
            update_heartbeat()
            logging.info(f"Batch complete: {processed_count}/{len(batch)} PDFs processed successfully")
            
            # Cleanup batch directory
            shutil.rmtree(current_batch_dir, ignore_errors=True)
            
            time.sleep(1)
            batch_count += 1

    except Exception as e:
        logging.error(f"Fatal error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
