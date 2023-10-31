import io
import os
from dotenv import load_dotenv
import pathlib
from zipfile import ZipFile, ZipInfo
from datetime import datetime
import pytz

# test
import tempfile
from tqdm import tqdm
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
DIR_PATH = os.getenv('DIR_PATH')

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
    if not folders_list:
        print(f'No folders found in folder with ID {folder_id}.')
        return

    # Debug ##############################################
    # for folder in folders_list:
    #     print(f"{folder['name']} ({folder['id']})")
    ######################################################

    if filter_by_mindate != "":
        folders_filtered = []
        for folder in folders_list:
            if folder['name'] >= filter_by_mindate:
                folders_filtered.append({'id' : folder['id'], 'name' : folder['name']})

        # Debug ##############################################
        # for folder in folders_filtered:
        #     print(f"{folder['name']} ({folder['id']})")
        ######################################################

        return folders_filtered
    else:
        return folders_list

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

def upload(credential_for_gcs, bucket_name, drive_service, folder_id, min_file_date):
    folder_list = getFoldersFromGDrive(drive_service, folder_id, min_file_date)
    for folder in folder_list:
        archive = io.BytesIO()
        with ZipFile(archive, 'w') as zip_archive:
            files = getFilesFromGDrive(drive_service, folder['id'], "")
            ## mp4 operation
            tmp = io.BytesIO()
            downloader = MediaIoBaseDownload(tmp, drive_service.files().get_media(fileId=files[0]['id']))
            done = False
            # Set up progress bar for downloading
            with tqdm(total=100, desc=f"Downloading {files[0]['name']}") as pbar:
                while not done:
                    status, done = downloader.next_chunk()
                    pbar.update(status.progress() * 100 - pbar.n)
            tmp.seek(0)
            print("archive")
            mp4 = ZipInfo(files[0]['name'])
            zip_archive.writestr(mp4, tmp.read())
            print("done")
            
            ## vtt operation
            tmp = io.BytesIO()
            downloader = MediaIoBaseDownload(tmp, drive_service.files().get_media(fileId=files[1]['id']))
            done = False
            with tqdm(total=100, desc=f"Downloading {files[0]['name']}") as pbar:
                while not done:
                    status, done = downloader.next_chunk()
                    pbar.update(status.progress() * 100 - pbar.n)
            tmp.seek(0)
            vtt = ZipInfo(files[1]['name'])
            zip_archive.writestr(vtt, tmp.read())
        archive.seek(0)

        storage_client = storage.Client.from_service_account_json(credential_for_gcs)
        bkt = storage_client.bucket(bucket_name)
        blob = bkt.blob(str(getCurrentTime('Asia/Tokyo'))+'.zip')
        print("uploading")
        blob.upload_from_file(archive, content_type='application/zip')
        print("done")

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
    upload(CREDENTIAL_GCS, BUCKET_NAME, drive_service, FOLDER_ID, "20231029")
