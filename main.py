import io
import os
from dotenv import load_dotenv
import pathlib
from zipfile import ZipFile, ZipInfo
from datetime import datetime
import pytz

# test
import tempfile

##

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload

from google.cloud import storage


# Variables
load_dotenv("./.env")
# drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIAL_OAUTH = os.getenv('CREDENTIAL_OAUTH')
FOLDER_ID = os.getenv('FOLDER_ID')
IS_MYDRIVE = False
# gcs
PROJECT_ID = os.getenv('PROJECT_ID')
CREDENTIAL_GCS =  os.getenv('CREDENTIAL_GCS')
BUCKET_NAME = os.getenv('BUCKET_NAME')

CONTENT_TYPES = {
            "mp4" : "video/mp4", 
            "vtt" : "text/vtt",
            "txt" : "text/plain; charset=utf-8",
        }


def authorizeApi(scopes, credential_path):
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(credential_path, scopes)
        creds = tools.run_flow(flow, store)
    return build('drive', 'v3', http=creds.authorize(Http()))

def getFoldersFromGDrive(drive_service ,folder_id, filter_by_mindate):
    filters = [
            f"'{folder_id}' in parents",
            # f"name contains '{filter_by_foldername}'",
            "mimeType = 'application/vnd.google-apps.folder'",
        ]
    query = " and ".join(filters)

    folders = drive_service.files().list(
        q=query,
        spaces= "drive",
        includeItemsFromAllDrives= True,
        supportsAllDrives=True,
        fields='nextPageToken, files(id, name)'
        ).execute()
    folders_list = folders.get('files', [])
    folders_list2 = []
    for folder in folders_list:
        if folder['name'] >= filter_by_mindate:
            folders_list2.append({'id' : folder['id'], 'name' : folder['name']})


    if not folders_list:
        print(f'No folders found in folder with ID {folder_id}.')
    else:
        print(f"Folders in the folder with ID {folder_id}:")
        for folder in folders_list2:
            print(f"{folder['name']} ({folder['id']})")
    return folders_list2

def getFilesFromGDrive(drive_service ,folder_id, filter_by_filename):
    filters = [
        f"'{folder_id}' in parents",
        f"name contains '{filter_by_filename}'",
        "mimeType != 'application/vnd.google-apps.folder'",
    ]
    query = " and ".join(filters)

    folders = drive_service.files().list(
        q=query,
        spaces= "drive",
        includeItemsFromAllDrives= True,
        supportsAllDrives=True,
        fields='nextPageToken, files(id, name)'
        ).execute()
    files_list = folders.get('files', [])

    if not files_list:
        print(f'No files found in folder with ID {folder_id}.')
    else:
        print(f"Files in the folder with ID {folder_id}:")
        for file in files_list:
            print(f"{file['name']} ({file['id']})")
    return files_list

def uploadFilesToGCS(file_name, file_id, credential, bucket_name, drive_service):
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_stream.seek(0)

    # Upload file stream to Google Cloud Storage
    storage_client = storage.Client.from_service_account_json(credential)
    bkt = storage_client.bucket(bucket_name)
    blob = bkt.blob(file_name)
    print(f"uploading {file_name}... It may take long time.\n")
    # Get file extension
    _, ext = os.path.splitext(file_name)
    blob.upload_from_file(file_stream, content_type=getContentType(ext[1:]),timeout=600)

    print(f"File from Google Drive uploaded to GCS at: gs://{bucket_name}/{file_name}")



def upload(credential, bucket_name, drive_service, folder_id):
    folders_list = getFoldersFromGDrive(drive_service, folder_id, "")
    archive = io.BytesIO()
    with ZipFile(archive, 'w') as zip_archive:
        for folder in folders_list:
            files = getFilesFromGDrive(drive_service, folder['id'], "")
            for file in files:
                print("start")
                downloader = MediaIoBaseDownload(archive, drive_service.files().get_media(fileId=file['id']))
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            archive.seek(0)
            object_name = str(getCurrentTime('Asia/Tokyo'))+'.zip'
            zip_entry_name = object_name
            zip_file = ZipInfo(zip_entry_name)
            file_data = archive.read()
            zip_archive.writestr(zip_file, file_data)
    archive.seek(0)


    storage_client = storage.Client.from_service_account_json(credential)
    bkt = storage_client.bucket(bucket_name)
    blob = bkt.blob(object_name)
    blob.upload_from_file(archive, content_type='application/zip')

def upload2(credential, bucket_name, drive_service, folder_id):
    folder_list = getFoldersFromGDrive(drive_service, folder_id, "")
    for folder in folder_list :
        file_list = getFilesFromGDrive(drive_service, folder['id'], "")
        with tempfile.TemporaryFile() as tmp_file:
            with ZipFile(tmp_file, 'w') as zip_file:
                for item in file_list:
                    request = drive_service.files().get_media(fileId=item['id'])
                    downloader = MediaIoBaseDownload(tmp_file, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        print(f"Downloaded {int(status.progress() * 100)}%.")
                        # The data is now in tmp_file, so we can add it to our zip.
                    tmp_file.seek(0)
                    zip_file.writestr(item['name'], tmp_file.read())
                    tmp_file.seek(0, os.SEEK_END)  # Move the pointer to the end for the next download.
            # Upload the temporary ZIP to Google Cloud Storage
            tmp_file.seek(0)
            storage_client = storage.Client.from_service_account_json(credential)
            bkt = storage_client.bucket(bucket_name)
            blob = bkt.blob(str(getCurrentTime('Asia/Tokyo'))+'.zip')
            blob.upload_from_file(tmp_file, content_type='application/zip')

def getContentType(extension):
    for t in CONTENT_TYPES :
        if t == extension :
            return CONTENT_TYPES.get(t)
    print("extension is not set")
    return None

def getCurrentTime(timezone):
    # Get the UTC+9 (Japan) timezone
    japan_tz = pytz.timezone(timezone)
    # Get the current time in UTC+9
    return datetime.now(japan_tz)


# Main Operation Below
if __name__ == '__main__':
    drive_service = authorizeApi(SCOPES, CREDENTIAL_OAUTH)
    # upload2(CREDENTIAL_GCS, BUCKET_NAME, drive_service, FOLDER_ID)

    getFoldersFromGDrive(drive_service, FOLDER_ID, "20231010")