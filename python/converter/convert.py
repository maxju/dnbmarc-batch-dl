import os
import logging
import requests
from typing import Optional, Tuple, List
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from nougat import NougatModel
from pathlib import Path
import torch
import torch_xla
import torch_xla.core.xla_model as xm
from dotenv import load_dotenv
from transformers import NougatProcessor, VisionEncoderDecoderModel

load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
LOCAL_TEMP_DIR = "temp_files"
MAX_WORKERS = 4  # Number of parallel workers
BATCH_SIZE = 100  # Number of records to process in each batch

# Initialize Nougat model
device = xm.xla_device()
processor = NougatProcessor.from_pretrained("facebook/nougat-base")
model = VisionEncoderDecoderModel.from_pretrained("facebook/nougat-base").to(device)
model.eval()

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

def convert_pdf_to_mmd(pdf_path: str) -> Optional[str]:
    """
    Convert a PDF file to Markdown using Nougat.
    
    Args:
        pdf_path: Local path of the PDF file
    
    Returns:
        Local path of the generated Markdown file, or None if conversion fails
    """
    try:
        pdf_path = Path(pdf_path)
        mmd_path = pdf_path.with_suffix('.mmd')

        with torch.no_grad():
            pixel_values = processor(images=pdf_path, return_tensors="pt").pixel_values.to(device) 
            generated_ids = model.generate(pixel_values)
            output = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
            output = processor.post_process_generation(output, fix_markdown=False)
        with open(mmd_path, 'w', encoding='utf-8') as f:
            f.write(output)

        logging.info(f"Successfully converted {pdf_path} to {mmd_path}")
        return str(mmd_path)
    except Exception as e:
        logging.error(f"Failed to convert PDF to Markdown: {str(e)}")
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
    Process a single PDF: download, convert, upload, and clean up.
    
    Args:
        drive_service: Authenticated Google Drive service object
        pdf_id: Unique identifier for the PDF
        pdf_url: URL of the PDF file
    
    Returns:
        True if the entire process was successful, False otherwise
    """
    pdf_path = download_pdf(pdf_url, pdf_id)
    if not pdf_path:
        return False

    mmd_path = convert_pdf_to_mmd(pdf_path)
    if not mmd_path:
        os.remove(pdf_path)
        return False

    success, file_id, filename = upload_file_to_drive(drive_service, mmd_path)
    os.remove(pdf_path)
    if success:
        mark_record_as_processed(pdf_id, file_id, filename)
        return True
    else:
        if os.path.exists(mmd_path):
            os.remove(mmd_path)
        return False

def process_batch(drive_service, batch: List[Tuple[str, str]]):
    """
    Process a batch of PDFs in parallel.
    
    Args:
        drive_service: Authenticated Google Drive service object
        batch: List of tuples containing (pdf_id, pdf_url)
    """
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pdf, drive_service, pdf_id, pdf_url) for pdf_id, pdf_url in batch]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    logging.info(f"Successfully processed a PDF")
                else:
                    logging.warning(f"Failed to process a PDF")
            except Exception as e:
                logging.error(f"Error processing PDF: {str(e)}")

def main():
    print(f'PyTorch can access {len(torch_xla.devices())} TPU cores')
    # Create Google Drive service
    drive_service = dfm.get_drive_service()

    # Ensure local temporary directory exists
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    # Process PDFs in batches
    for batch in get_pdf_links(BATCH_SIZE):
        logging.info(f"Processing batch of {len(batch)} PDFs")
        process_batch(drive_service, batch)
        time.sleep(1)  # Add a small delay between batches to avoid overwhelming the system

    logging.info("Conversion and upload process completed.")

if __name__ == "__main__":
    main()