from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

from google.cloud import storage


# drive
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIAL_OAUTH = './credential_OAuth.json'
FOLDER_ID = '1wdiC8e3fKpDkvdIQvf_9-63fIeDCsxR_'

# gcs
PROJECT_ID = 'sandbox-ido'
CREDENTIAL_GCS = './credential_gcs_sa.json'
BUCKET_NAME = 'camera_data_rcv_test_bucket'


def authorizeApi(scopes, credential_path):
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(credential_path, scopes)
        creds = tools.run_flow(flow, store)
    return build('drive', 'v3', http=creds.authorize(Http()))

drive_service = authorizeApi(SCOPES, CREDENTIAL_OAUTH)

def getFilesFromGDrive(folder_id, file_name, isMyDrive):
    # file_name = 'test'
    filters = [
        f"'{folder_id}' in parents",
        f"name contains '{file_name}'",
        "mimeType != 'application/vnd.google-apps.folder'"
    ]
    query = " and ".join(filters)

    folders = drive_service.files().list(
        q=query,
        spaces= "root" if isMyDrive else "drive",
        includeItemsFromAllDrives= False if isMyDrive else True,
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


# upload to gcs
storage_client = storage.Client.from_service_account_json(CREDENTIAL_GCS)
# buckets = list(storage_client.list_buckets())
# print(buckets)

# Create a test file
# with open('./test.txt', 'w') as f:
#     f.write('This is a test file for GCS!')

bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob('test.txt')

with open('./test.txt', 'rb') as f:
    blob.upload_from_file(f)

# Upload the test file to GCS
