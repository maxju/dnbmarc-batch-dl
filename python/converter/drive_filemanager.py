from __future__ import print_function
import os
import logging
import mimetypes
from typing import Optional
import random
import string
import tempfile

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# If modifying these scopes, update the scope in your Google Cloud Console
SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "google-service-account.json"


def get_credentials() -> Optional[service_account.Credentials]:
    """
    Retrieve Google Drive API credentials from the service account file.
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        return creds
    except Exception as e:
        logging.error(f"Error loading service account credentials: {e}")
        return None


def get_drive_service():
    """
    Create and return an authenticated Google Drive service object.
    """
    creds = get_credentials()
    if not creds:
        raise ValueError("Failed to obtain valid credentials")
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_folder_contents(service, folder_id: str):
    """
    List the contents (files and folders) of a specific Google Drive folder.

    Args:
        service: Authenticated Google Drive service object.
        folder_id: ID of the folder to list contents from.

    Returns:
        A list of dictionaries containing file/folder information.
    """
    query = f"'{folder_id}' in parents"
    results = (
        service.files()
        .list(pageSize=1000, q=query, fields="nextPageToken, files(id, name, mimeType)")
        .execute()
    )
    return results.get("files", [])


def download_file(
    service, file_id: str, file_name: str, destination: str, skip_existing: bool = False
):
    """
    Download a file from Google Drive to a local destination.

    Args:
        service: Authenticated Google Drive service object.
        file_id: ID of the file to download.
        file_name: Name to save the file as.
        destination: Local directory to save the file.
        skip_existing: If True, skip download if file already exists locally.
    """
    file_path = os.path.join(destination, file_name)
    if skip_existing and os.path.exists(file_path):
        print(f"Skipping existing file: {file_name}")
        return

    request = service.files().get_media(fileId=file_id)
    with open(file_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading {file_name}: {int(status.progress() * 100)}%")


def create_folder(service, folder_name: str, parent_id: str) -> str:
    """
    Create a new folder in Google Drive.

    Args:
        service: Authenticated Google Drive service object.
        folder_name: Name of the new folder.
        parent_id: ID of the parent folder.

    Returns:
        The ID of the newly created folder.
    """
    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=folder_metadata, fields="id").execute()
    return folder.get("id")


def upload_file(service, file_path: str, parent_id: str, skip_existing: bool = False):
    """
    Upload a file to Google Drive.

    Args:
        service: Authenticated Google Drive service object.
        file_path: Local path of the file to upload.
        parent_id: ID of the parent folder in Google Drive.
        skip_existing: If True, skip upload if file already exists in Drive.

    Returns:
        Tuple of (file_id: str, filename: str) of the uploaded file
    """
    file_name = os.path.basename(file_path)

    if skip_existing:
        existing_files = list_folder_contents(service, parent_id)
        if any(file["name"] == file_name for file in existing_files):
            print(f"Skipping existing file: {file_name}")
            return None, None

    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type is None:
        mime_type = "application/octet-stream"

    file_metadata = {"name": file_name, "parents": [parent_id]}
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    print(f"Uploaded file: {file_name}")
    return file.get("id"), file_name


def download_folder_recursive(
    service, folder_id: str, destination: str, skip_existing: bool = False
):
    """
    Recursively download a folder and its contents from Google Drive.

    Args:
        service: Authenticated Google Drive service object.
        folder_id: ID of the folder to download.
        destination: Local directory to save the downloaded contents.
        skip_existing: If True, skip download of existing files/folders.
    """
    if not os.path.exists(destination):
        os.makedirs(destination)

    items = list_folder_contents(service, folder_id)
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            subfolder_path = os.path.join(destination, item["name"])
            if skip_existing and os.path.exists(subfolder_path):
                print(f"Skipping existing folder: {item['name']}")
                continue
            print(f"Folder: {item['name']}")
            os.makedirs(subfolder_path, exist_ok=True)
            download_folder_recursive(
                service, item["id"], subfolder_path, skip_existing
            )
        else:
            download_file(service, item["id"], item["name"], destination, skip_existing)
            print(f"File: {item['name']}")


def upload_folder_recursive(
    service, local_folder: str, parent_id: str, skip_existing: bool = False
):
    """
    Recursively upload a local folder and its contents to Google Drive.

    Args:
        service: Authenticated Google Drive service object.
        local_folder: Path to the local folder to upload.
        parent_id: ID of the parent folder in Google Drive.
        skip_existing: If True, skip upload of existing files/folders.
    """
    existing_items = list_folder_contents(service, parent_id) if skip_existing else []

    for item in os.listdir(local_folder):
        item_path = os.path.join(local_folder, item)
        if os.path.isdir(item_path):
            existing_folder = next(
                (
                    f
                    for f in existing_items
                    if f["name"] == item
                    and f["mimeType"] == "application/vnd.google-apps.folder"
                ),
                None,
            )
            if existing_folder and skip_existing:
                print(f"Skipping existing folder: {item}")
                folder_id = existing_folder["id"]
            else:
                folder_id = create_folder(service, item, parent_id)
                print(f"Created folder: {item}")
            upload_folder_recursive(service, item_path, folder_id, skip_existing)
        else:
            upload_file(service, item_path, parent_id, skip_existing)


if __name__ == "__main__":
    # Set up the Drive service
    service = get_drive_service()

    # Main directory ID
    main_dir_id = "1wl9M0ZYHnZSK_IXcQDLheeyY1v9ldTAZ" # DNB Directory

    # Create a random suffix for the folder name
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    folder_name = f"sample_folder_{random_suffix}"

    # Create the folder in Google Drive
    folder_id = create_folder(service, folder_name, main_dir_id)
    print(f"Created folder: {folder_name}")

    # Create a temporary file with random content
    random_filename = ''.join(random.choices(string.ascii_lowercase, k=8)) + ".txt"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
        temp_file.write(f"This is a sample file created at {tempfile.gettempdir()}")
        temp_file_path = temp_file.name

    # Upload the temporary file to the newly created folder
    file_id, file_name = upload_file(service, temp_file_path, folder_id, skip_existing=False)
    print(f"Uploaded file: {file_name}")

    # Clean up the temporary file
    os.remove(temp_file_path)

    print(f"Sample folder '{folder_name}' with a random text file has been created and uploaded successfully.")

    # Create a download folder
    download_folder = os.path.join(os.getcwd(), "download_folder")
    os.makedirs(download_folder, exist_ok=True)

    # Download the random file
    download_file(service, file_id, file_name, download_folder, skip_existing=False)
    print(f"Downloaded file: {file_name} to {download_folder}")

    print(f"Sample folder '{folder_name}' with a random text file has been created, uploaded, and downloaded successfully.")
