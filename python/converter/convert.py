import os
import logging
import sys
from collections import deque
import requests
from typing import Optional, Tuple, List, Set
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed
import time
from pathlib import Path
from dotenv import load_dotenv
from marker.convert import convert_single_pdf
from marker.models import load_all_models
import torch.multiprocessing as mp
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
MAX_WORKERS = 5
BATCH_SIZE = 60
BLACKLIST_FILE = "failed_pdfs.json"

def load_blacklist() -> set:
    """
    Load the blacklist of failed PDF IDs from file
    """
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logging.error(f"Error loading blacklist: {e}")
    return set()

def add_to_blacklist(pdf_id: str):
    """
    Add a PDF ID to the blacklist
    """
    blacklist = load_blacklist()
    blacklist.add(pdf_id)
    try:
        with open(BLACKLIST_FILE, 'w') as f:
            json.dump(list(blacklist), f, indent=2)
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
                add_to_blacklist(pdf_id)
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
    add_to_blacklist(pdf_id)
    return False

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

def worker_init(shared_model):
    if shared_model is None:
        shared_model = load_all_models()

    global model_refs
    model_refs = shared_model

def worker_exit():
    global model_refs
    del model_refs

def process_pdf(model_lst, drive_service, pdf_id: str, pdf_url: str) -> bool:
    """
    Process a single PDF with validation and blacklist checks.
    """
    # Check blacklist first
    blacklist = load_blacklist()
    if pdf_id in blacklist:
        logging.info(f"Skipping blacklisted PDF {pdf_id}")
        mark_record_as_processed(pdf_id, None, "Blacklisted")
        return False

    # Initialize paths
    pdf_path = None
    mmd_path = None
    
    try:
        local_path = os.path.join(LOCAL_TEMP_DIR, f"{pdf_id}.pdf")
        if not download_with_retry(pdf_id, pdf_url, local_path):
            return False
        pdf_path = local_path

        # Convert PDF
        markdown_text, _, _ = convert_single_pdf(
            pdf_path, 
            model_lst
        )

        mmd_path = pdf_path.replace('.pdf', '.mmd')
        with open(mmd_path, 'w', encoding='utf-8') as f:
            f.write(markdown_text)

        success, file_id, filename = upload_file_to_drive(drive_service, mmd_path)
        
        if success:
            mark_record_as_processed(pdf_id, file_id, filename)
            return True
        else:
            add_to_blacklist(pdf_id)
            return False
                
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Error processing PDF {pdf_id}: {error_msg}")
        add_to_blacklist(pdf_id)
        return False
    finally:
        # Cleanup
        try:
            if pdf_path and os.path.exists(pdf_path):
                os.remove(pdf_path)
            if mmd_path and os.path.exists(mmd_path):
                os.remove(mmd_path)
        except Exception as cleanup_error:
            logging.error(f"Error during cleanup for PDF {pdf_id}: {str(cleanup_error)}")

def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Add heartbeat file management
    heartbeat_file = "converter_heartbeat.txt"
    def update_heartbeat():
        with open(heartbeat_file, 'w') as f:
            f.write(str(time.time()))

    try:
        logging.info(f"Using {MAX_WORKERS} workers and batch size {BATCH_SIZE}")
        # Configure tqdm to write to stderr
        tqdm.write = partial(print, file=sys.stderr)
        drive_service = dfm.get_drive_service()
        
        # Create batch-specific temp directories
        os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

        try:
            mp.set_start_method('spawn')  # Required for CUDA
        except RuntimeError:
            raise RuntimeError("Failed to set start method to spawn. Please try running again.")

        # Initialize models
        model_lst = load_all_models()
        if model_lst is not None:
            for model in model_lst:
                if model is not None:
                    model.share_memory()

        batch_count = 0
        last_successful_conversion = time.time()

        # Process PDFs in batches
        for batch in get_pdf_links(BATCH_SIZE):
            update_heartbeat()
            
            if not batch:
                logging.info("No PDFs to process in batch. Exiting.")
                sys.exit(0)
                
            logging.info(f"Processing batch number {batch_count} containing {len(batch)} PDFs")
            
            task_args = [(model_lst, drive_service, pdf_id, pdf_url) for pdf_id, pdf_url in batch]
            total_pdfs = len(batch)
            
            # Use torch multiprocessing
            with mp.Pool(processes=MAX_WORKERS, initializer=worker_init, initargs=(model_lst,)) as pool:
                # Process PDFs with progress bar
                results = list(tqdm(
                    pool.starmap(process_pdf, task_args),
                    total=total_pdfs,
                    desc="Processing PDFs",
                    unit="pdf"
                ))
                
                # Ensure proper cleanup
                pool._worker_handler.terminate = worker_exit
                
                # Count successful conversions
                processed_count = sum(1 for result in results if result)
            
            if processed_count > 0:
                last_successful_conversion = time.time()
            elif time.time() - last_successful_conversion > 900:  # 15 minutes
                logging.error("No successful conversions in 15 minutes. Exiting.")
                sys.exit(1)
            
            update_heartbeat()
            logging.info(f"Batch complete: {processed_count}/{total_pdfs} PDFs processed successfully")
            
            time.sleep(1)
            batch_count += 1

        logging.info("Conversion and upload process completed.")
        del model_lst

    except Exception as e:
        logging.error(f"Fatal error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
