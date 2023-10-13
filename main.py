from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

store = file.Storage('token.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('./client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
drive_service = build('drive', 'v3', http=creds.authorize(Http()))



# About Google Drive
FOLDER_ID = ""
# About GCS Info
GCS_PROJECT_ID = ""
BUCKET_NAME = ""
SA_CRED = ""
SA_EMAIL = ""

def myFunction() :
  transferDriveFilesToGCS(FOLDER_ID)

def transferDriveFilesToGCS(folderId, bucketName, projectId, serviceAccount):
  folder = DriveApp.getFolderById(folderId)
  files = folder.getFiles()
  
  while (files.hasNext()):
    file = files.next()
    blob = file.getBlob()
    # Upload to GCS
    uploadToGCS(blob, file.getName(), bucketName, projectId, serviceAccount)

def uploadToGCS(blob, fileName, bucketName, projectId, serviceAccount):
  url = "https://storage.googleapis.com/upload/storage/v1/b/" + bucketName + "/o?uploadType=media&name=" + fileName
  
    headers = {
    "Authorization": "Bearer " + ScriptApp.getOAuthToken(),
    "x-goog-project-id": projectId
    }
   options = {
    "method": "POST",
    "contentType": blob.getContentType(),
    "payload": blob.getBytes(),
    "headers": headers,
    "muteHttpExceptions": true
  }

  response = UrlFetchApp.fetch(url, options)
  Logger.log(response)

