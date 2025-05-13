from google.cloud import storage
from google.oauth2 import service_account
import json

class Storage_Manager:

   def __init__(self, key_file_path):
      self.credentials = service_account.Credentials.from_service_account_file(key_file_path)
      self.storage_manager = storage.Client(credentials=self.credentials)

   def upload_json_to_gcs(self, bucket_name, json_data, destination_blob):
      bucket = self.storage_manager.bucket(bucket_name)
      blob = bucket.blob(destination_blob)
      
      # Python 객체를 JSON 문자열로 변환
      json_string = json.dumps(json_data)
      
      # JSON 문자열 업로드
      blob.upload_from_string(json_string, content_type='application/json')

      return True