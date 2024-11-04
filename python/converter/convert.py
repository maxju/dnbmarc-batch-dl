import os
import logging
import requests
from typing import Optional, Tuple, List
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from pathlib import Path
from dotenv import load_dotenv
from marker.convert import convert_single_pdf
from marker.models import load_all_models
from marker.utils import flush_cuda_memory
from itertools import cycle
import torch

os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"  # For M1/M2 Macs
os.environ["EXTRACT_IMAGES"] = "false"  # Disable image extraction

load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
LOCAL_TEMP_DIR = "temp_files"
# TORCH_DEVICE = "mps"

# Get available GPUs
gpu_count = torch.cuda.device_count()
gpu_ids = list(range(gpu_count))
gpu_cycle = cycle(gpu_ids)

MAX_WORKERS = gpu_count * 4
BATCH_SIZE = MAX_WORKERS * 4

logging.info(f"Using {gpu_count} GPUs with {MAX_WORKERS} workers and batch size {BATCH_SIZE}")

# Load models for each GPU
model_lst_per_gpu = {}
for gpu_id in gpu_ids:
    with torch.cuda.device(gpu_id):
        model_lst_per_gpu[gpu_id] = load_all_models()
        logging.info(f"Loaded models on GPU {gpu_id}")

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
            os.remove(file_path)
            logging.info(f"Successfully uploaded and deleted: {file_path}")
            return True, file_id, filename
        else:
            logging.error(f"Failed to upload {file_path}: file_id or filename is None")
            return False, None, None
    except Exception as e:
        logging.error(f"Failed to upload {file_path}: {str(e)}")
        return False, None, None

def process_pdf(drive_service, pdf_id: str, pdf_url: str) -> bool:
    """
    Process a single PDF using round-robin GPU assignment.
    """
    gpu_id = next(gpu_cycle)  # Get next GPU in rotation
    torch.cuda.set_device(gpu_id)
    
    pdf_path = download_pdf(pdf_url, pdf_id)
    if not pdf_path:
        return False

    try:
        # Convert PDF using models from assigned GPU
        markdown_text, _, _ = convert_single_pdf(
            pdf_path, 
            model_lst_per_gpu[gpu_id]
        )
        
        mmd_path = pdf_path.replace('.pdf', '.mmd')
        with open(mmd_path, 'w', encoding='utf-8') as f:
            f.write(markdown_text)

        # Clear GPU memory after processing
        flush_cuda_memory()

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
        logging.error(f"Error converting PDF {pdf_id} on GPU {gpu_id}: {str(e)}")
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
        flush_cuda_memory()  # Make sure to clear memory on error
        return False

def process_pdf_with_retry(drive_service, pdf_id: str, pdf_url: str) -> bool:
    """
    Process a PDF with retry logic for OOM errors
    """
    for attempt in range(gpu_count):
        try:
            return process_pdf(drive_service, pdf_id, pdf_url)
        except torch.cuda.OutOfMemoryError:
            flush_cuda_memory()
            continue
        except Exception as e:
            logging.error(f"Error on attempt {attempt}: {str(e)}")
            continue
    return False

def process_batch(drive_service, batch: List[Tuple[str, str]]):
    """
    Process a batch of PDFs in parallel.
    """
    optimal_workers = gpu_count * 2  # Use 2 workers per GPU
    
    with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
        futures = [
            executor.submit(process_pdf_with_retry, drive_service, pdf_id, pdf_url) 
            for pdf_id, pdf_url in batch
        ]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    logging.info("Successfully processed a PDF")
                else:
                    logging.warning("Failed to process a PDF")
            except Exception as e:
                logging.error(f"Error processing PDF: {str(e)}")

def main():
    logging.info(f"Starting converter.py with {gpu_count} GPUs")
    for gpu_id in gpu_ids:
        gpu_name = torch.cuda.get_device_name(gpu_id)
        gpu_memory = torch.cuda.get_device_properties(gpu_id).total_memory / 1e9  # Convert to GB
        logging.info(f"GPU {gpu_id}: {gpu_name} with {gpu_memory:.2f}GB memory")

    # Create Google Drive service
    drive_service = dfm.get_drive_service()

    # Ensure local temporary directory exists
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    batch_count = 0
    # Process PDFs in batches
    for batch in get_pdf_links(BATCH_SIZE):
        logging.info(f"Processing batch number {batch_count} containing {len(batch)} PDFs")
        process_batch(drive_service, batch)
        torch.cuda.empty_cache()  # Clear GPU memory after each batch
        time.sleep(1)  # Small delay between batches
        batch_count += 1

    logging.info("Conversion and upload process completed.")

if __name__ == "__main__":
    main()