from google.cloud import storage
import urllib.request

project_id = 'evident-display-369507'
bucket_name = 'project-data-fellowship'
destination_blob_name = 'upload-file'
storage_client = storage.Client.from_service_account_json(
    'evident-display-369507-f9e45416218e.json')

source_file_name = 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/9f/Jeon_Jungkook_at_the_White_House%2C_31_May_2022.jpg/220px-Jeon_Jungkook_at_the_White_House%2C_31_May_2022.jpg'


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    link = urllib.request.urlopen(source_file_name)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(link.read(), content_type='image/jpg')


upload_blob(bucket_name, source_file_name, destination_blob_name)
