import io
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

SERVICE_ACCOUNT_FILE = "/Volumes/workspace/default/bmtceta/gtfs-ingestion-5f751c6669b8.json"

FOLDER_ID = "1pa1kzq6L-nZDpsX7LSr7wmtQPZhbwnWA"

DOWNLOAD_FOLDER = "/Volumes/workspace/default/bmtceta/bmtc/raw"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly"
]


# --------------------------------------------------
# AUTHENTICATION
# --------------------------------------------------

def authenticate_drive():
    """
    Authenticate with Google Drive.

    Returns
    -------
    googleapiclient.discovery.Resource
    """

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

    drive_service = build(
        "drive",
        "v3",
        credentials=credentials
    )

    return drive_service


# --------------------------------------------------
# LIST FILES
# --------------------------------------------------

def list_drive_files():
    """
    List every file inside the configured Drive folder.

    Returns
    -------
    list
    """

    drive_service = authenticate_drive()

    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        pageSize=1000,
        fields="files(id,name,size)"
    ).execute()

    files = results.get("files", [])

    files = sorted(
        files,
        key=lambda x: x["name"]
    )

    return files


# --------------------------------------------------
# DOWNLOAD ONE FILE
# --------------------------------------------------

def download_file(file_id, file_name):
    """
    Download one Google Drive file.

    Parameters
    ----------
    file_id : str

    file_name : str

    Returns
    -------
    str
        Local file path
    """

    drive_service = authenticate_drive()

    os.makedirs(
        DOWNLOAD_FOLDER,
        exist_ok=True
    )

    local_path = os.path.join(
        DOWNLOAD_FOLDER,
        file_name
    )

    request = drive_service.files().get_media(
        fileId=file_id
    )

    with io.FileIO(local_path, "wb") as fh:

        downloader = MediaIoBaseDownload(
            fh,
            request
        )

        done = False

        while not done:

            status, done = downloader.next_chunk()

            if status:

                print(
                    f"{file_name} : {int(status.progress()*100)}%"
                )

    print(f"Downloaded -> {local_path}")

    return local_path


# --------------------------------------------------
# DOWNLOAD MULTIPLE FILES
# --------------------------------------------------

def download_files(max_files=None):
    """
    Download multiple files.

    Parameters
    ----------
    max_files : int or None

        None -> download every file

    Returns
    -------
    list
        List of downloaded local paths
    """

    files = list_drive_files()

    if max_files is not None:

        files = files[:max_files]

    downloaded_files = []

    for file in files:

        local_path = download_file(
            file["id"],
            file["name"]
        )

        downloaded_files.append(local_path)

    return downloaded_files


# --------------------------------------------------
# DELETE FILE
# --------------------------------------------------

def delete_local_file(local_path):
    """
    Delete one downloaded parquet file.
    """

    import os

    if os.path.isfile(local_path):
        os.remove(local_path)
        print(f"Deleted -> {local_path}")


# --------------------------------------------------
# DELETE MULTIPLE FILES
# --------------------------------------------------

def delete_local_files(file_paths):
    """
    Delete multiple downloaded files.
    """

    for path in file_paths:

        delete_local_file(path)