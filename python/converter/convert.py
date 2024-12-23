import os
import logging
import sys
from collections import deque
import requests
from typing import Optional, Tuple, List
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

os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"  # For M1/M2 Macs
os.environ["EXTRACT_IMAGES"] = "false"  # Disable image extraction
os.environ["IN_STREAMLIT"] = "true" # Avoid multiprocessing inside surya
os.environ["PDFTEXT_CPU_WORKERS"] = "1" # Avoid multiprocessing inside pdftext


# Constants
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
LOCAL_TEMP_DIR = "temp_files"
BATCH_SIZE = 5

def download_batch(batch: List[Tuple[str, str]], temp_dir: str) -> List[Tuple[str, str, str]]:
    """
    Download a batch of PDFs and return list of (pdf_id, pdf_url, local_path)
    """
    downloaded = []
    with tqdm(total=len(batch), desc="Downloading PDFs", unit="pdf") as pbar:
        for pdf_id, pdf_url in batch:
            local_path = os.path.join(temp_dir, f"{pdf_id}.pdf")
            try:
                response = requests.get(pdf_url, stream=True)
                response.raise_for_status()
                with open(local_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                downloaded.append((pdf_id, pdf_url, local_path))
                pbar.update(1)
            except Exception as e:
                logging.error(f"Failed to download PDF {pdf_id}: {str(e)}")
                if os.path.exists(local_path):
                    os.remove(local_path)
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
            try:
                # Convert individual PDF
                rendered = converter(pdf_path)
                markdown_text, _, _ = text_from_rendered(rendered)
                
                # Save markdown
                mmd_path = pdf_path.replace('.pdf', '.mmd')
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
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                if os.path.exists(mmd_path):
                    os.remove(mmd_path)
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
