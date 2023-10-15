import io
import os
from dotenv import load_dotenv

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload

from google.cloud import storage


# drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIAL_OAUTH = os.getenv('CREDENTIAL_OAUTH')
FOLDER_ID = os.getenv('FOLDER_ID')

# gcs
PROJECT_ID = os.getenv('PROJECT_ID')
CREDENTIAL_GCS =  os.getenv('CREDENTIAL_GCS')
BUCKET_NAME = os.getenv('BUCKET_NAME')


def authorizeApi(scopes, credential_path):
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(credential_path, scopes)
        creds = tools.run_flow(flow, store)
    return build('drive', 'v3', http=creds.authorize(Http()))

def getFilesFromGDrive(folder_id, filter_by_filename, isMyDrive):
    filters = [
        f"'{folder_id}' in parents",
        f"name contains '{filter_by_filename}'",
        "mimeType != 'application/vnd.google-apps.folder'"
    ]
    query = " and ".join(filters)

    folders = drive_service.files().list(
        q=query,
        # spaces= "root" if isMyDrive else "drive",
        spaces= "drive",
        includeItemsFromAllDrives= False if isMyDrive else True,
        supportsAllDrives=True,
        fields='nextPageToken, files(id, name)'
        ).execute()
    files_list = folders.get('files', [])

    if not files_list:
        print(f'No files found in folder with ID {folder_id}.')
    # else:
    #     print(f"Files in the folder with ID {folder_id}:")
    #     for file in files_list:
    #         print(f"{file['name']} ({file['id']})")
    return files_list

def uploadFilesToGCS(file_name, file_id, credential, bucket_name):
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
    blob.upload_from_file(file_stream, content_type='video/mp4',timeout=600)

    print(f"File from Google Drive uploaded to GCS at: gs://{bucket_name}/{file_name}")


# Main Operation Below
drive_service = authorizeApi(SCOPES, CREDENTIAL_OAUTH)
filelists = getFilesFromGDrive(FOLDER_ID, "", True)

for f in filelists :
    uploadFilesToGCS(f['name'], f['id'], CREDENTIAL_GCS, BUCKET_NAME)