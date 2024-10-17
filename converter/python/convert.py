import os
import logging
import requests
from typing import Optional, Tuple
import drive_filemanager as dfm
from get_records import get_pdf_links, mark_record_as_processed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
# TODO: Replace with your actual Google Drive folder ID
DRIVE_FOLDER_ID = "your_google_drive_folder_id_here"
LOCAL_TEMP_DIR = "temp_files"

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
    Convert a PDF file to Markdown using Meta Nougat.
    
    Args:
        pdf_path: Local path of the PDF file
    
    Returns:
        Local path of the generated Markdown file, or None if conversion fails
    """
    # TODO: Implement PDF to Markdown conversion using Meta Nougat
    # 1. Use Meta Nougat to convert PDF to Markdown
    # 2. Save the Markdown content to a .mmd file
    # 3. Return the path of the generated .mmd file
    logging.error("PDF to Markdown conversion not implemented yet")
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
        os.remove(file_path)
        logging.info(f"Successfully uploaded and deleted: {file_path}")
        return True, file_id, filename
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

def main():
    # Create Google Drive service
    drive_service = dfm.get_drive_service()

    # Ensure local temporary directory exists
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    # Get PDF links from database
    pdf_links = get_pdf_links()

    for pdf_id, pdf_url in pdf_links:
        try:
            if process_pdf(drive_service, pdf_id, pdf_url):
                logging.info(f"Successfully processed and marked as converted: {pdf_id}")
            else:
                logging.warning(f"Failed to process: {pdf_id}")
        except Exception as e:
            logging.error(f"Error processing {pdf_id}: {str(e)}")

    logging.info("Conversion and upload process completed.")

if __name__ == "__main__":
    main()
