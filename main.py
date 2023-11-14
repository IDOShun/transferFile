import io
import os
from dotenv import load_dotenv
import zipfile
from datetime import datetime
import pytz
from tqdm import tqdm # for progress bar
import tempfile
import shutil


from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage


###### Variables ######
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
# DIR_PATH = os.getenv('DIR_PATH')
##########################

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

def makeArchiveOfAFolder(folder, drive_service):
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_list = getFilesFromGDrive(drive_service, folder['id'], "")
        dir_name = folder['name']
        for file in file_list:
            file_io = io.BytesIO()
            downloader = MediaIoBaseDownload(file_io, drive_service.files().get_media(fileId=file['id']))
            done = False
            # progress bar
            with tqdm(total=100, desc=f"Downloading {file['name']}") as pbar:
                while not done:
                    status, done = downloader.next_chunk()
                    pbar.update(status.progress() * 100 - pbar.n)
            file_io.seek(0)
            os.makedirs(os.path.join(tmp_dir, dir_name), exist_ok=True)
            file_path = os.path.join(tmp_dir, dir_name, file['name'])
            # copy file
            with open(file_path, 'wb') as temp_file:
                temp_file.write(file_io.read())

        # Create a BytesIO object to store the ZIP file
        zip_buffer = io.BytesIO()
        print("creating archive...")
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for dir_path, dirnames, filenames in os.walk(tmp_dir):
                # when file is exist in specified directory in a temp directory
                if filenames:
                    dir_name = dir_path.split('/')[-1]
                    for file_name in filenames:
                        file_path = os.path.join(dir_name, file_name)
                        zipf.write(os.path.join(tmp_dir, file_path), file_path)
        print("done.")
        zip_buffer.seek(0)
        return zip_buffer

def upload(credential_for_gcs, bucket_name, archive, dirpath=''):
    archive.seek(0)
    storage_client = storage.Client.from_service_account_json(credential_for_gcs)
    bkt = storage_client.bucket(bucket_name)
    blob = bkt.blob(str(dirpath)+str(getCurrentTime())+'.zip')
    print("uploading to gcs...")
    blob.upload_from_file(archive, content_type='application/zip')
    print("done.")

def getCurrentTime(timezone='Asia/Tokyo'):
    '''
    Default timezone is set to Japan (UTC+9)
    '''
    return datetime.now(pytz.timezone(timezone)).strftime('%Y%m%d%H%M%S%f')

# Main Operation Below
if __name__ == '__main__':
    drive_service = authorizeApi(SCOPES, CREDENTIAL_OAUTH)
    folder_list = getFoldersFromGDrive(drive_service, FOLDER_ID, "20231111")
    for folder in folder_list:
        archive = makeArchiveOfAFolder(folder, drive_service)
        upload(CREDENTIAL_GCS, BUCKET_NAME, archive)