import os
import logging
from collections import deque
import requests
from typing import Optional, Tuple, List
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from pathlib import Path
from dotenv import load_dotenv
from marker.convert import convert_single_pdf
from marker.settings import settings
from marker.models import load_all_models
from marker.utils import flush_cuda_memory
from itertools import cycle
import torch.multiprocessing as mp
from tqdm import tqdm

os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"  # For M1/M2 Macs
os.environ["EXTRACT_IMAGES"] = "false"  # Disable image extraction
os.environ["IN_STREAMLIT"] = "true" # Avoid multiprocessing inside surya
os.environ["PDFTEXT_CPU_WORKERS"] = "1" # Avoid multiprocessing inside pdftext


# Constants
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
LOCAL_TEMP_DIR = "temp_files"
# TORCH_DEVICE = "mps"

MAX_WORKERS = 4
BATCH_SIZE = 40

def download_pdf(pdf_url: str, pdf_id: str) -> Optional[str]:
    """
    Download a PDF file from the given URL and save it locally.
    
    Args:
        pdf_url: URL of the PDF file
        pdf_id: Unique identifier for the PDF
    
    Returns:
        Local path of the downloaded PDF file, or None if download fails
    """
    local_filename = os.path.join(LOCAL_TEMP_DIR, f"{pdf_id}.pdf")
    try:
        response = requests.get(pdf_url, stream=True)
        response.raise_for_status()
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logging.info(f"Successfully downloaded: {local_filename}")
        return local_filename
    except requests.RequestException as e:
        logging.error(f"Failed to download PDF {pdf_id}: {str(e)}")
        return None


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


def process_pdf(model_lst, drive_service, pdf_id: str, pdf_url: str) -> bool:
    """
    Process a single PDF.
    """
    pdf_path = download_pdf(pdf_url, pdf_id)
    if not pdf_path:
        return False

    try:
        # Convert PDF
        markdown_text, _, _ = convert_single_pdf(
            pdf_path, 
            model_lst
        )

        mmd_path = pdf_path.replace('.pdf', '.mmd')
        with open(mmd_path, 'w', encoding='utf-8') as f:
            f.write(markdown_text)

        success, file_id, filename = upload_file_to_drive(drive_service, mmd_path)
        os.remove(pdf_path)

        if success:
            mark_record_as_processed(pdf_id, file_id, filename)
            return True
        else:
            if os.path.exists(mmd_path):
                os.remove(mmd_path)
            return False
                
    except Exception as e:
        logging.error(f"Error converting PDF {pdf_id}: {str(e)}")
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        return False


def worker_init(shared_model):
    if shared_model is None:
        shared_model = load_all_models()

    global model_refs
    model_refs = shared_model


def worker_exit():
    global model_refs
    del model_refs


def main():
    load_dotenv()
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(f"Using {MAX_WORKERS} workers and batch size {BATCH_SIZE}")
    # Create Google Drive service
    drive_service = dfm.get_drive_service()

    # Ensure local temporary directory exists
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    try:
        mp.set_start_method('spawn') # Required for CUDA, forkserver doesn't work
    except RuntimeError:
        raise RuntimeError("Set start method to spawn twice. This may be a temporary issue with the script. Please try running it again.")

    if settings.TORCH_DEVICE == "mps" or settings.TORCH_DEVICE_MODEL == "mps":
        print("Cannot use MPS with torch multiprocessing share_memory. This will make things less memory efficient. If you want to share memory, you have to use CUDA or CPU.  Set the TORCH_DEVICE environment variable to change the device.")
        model_lst = None
    else:
        model_lst = load_all_models()

        for model in model_lst:
            if model is None:
                continue
            model.share_memory()

    batch_count = 0
    # Process PDFs in batches
    for batch in get_pdf_links(BATCH_SIZE):
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
        
        logging.info(f"Batch complete: {processed_count}/{total_pdfs} PDFs processed successfully")
        time.sleep(1)  # Small delay between batches
        batch_count += 4

    logging.info("Conversion and upload process completed.")
    del model_lst


if __name__ == "__main__":
    main()
