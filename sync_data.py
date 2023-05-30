import os
import json

from pcloud import PyCloud
from dotenv import load_dotenv
from urllib.request import urlopen
from datetime import date

__DOWNLOAD_DATA_PATH__ = "./data.json"
__DOWNLOAD_FILE_FOLDER_PATH__ = "./tmp"
__REMOTE_FOLDER_BASE_PATH__ = "datasets"
__REMOTE_FOLDER_PROJECT_PATH__ = "italy_gas_station_data"

load_dotenv()

class HandledPyCloud(PyCloud):

  def createhandledfolderifnotexists(self, parent_folder_id, folder_name):
    res = super().createfolderifnotexists(folderid=parent_folder_id, name=folder_name)
    if 'error' in res:
      print(res['error'])
      exit(res['result'])
    return res

def download_url(url: str, path: str):
  body = None
  with urlopen(url) as response:
    body = response.read()
  with open(path, "wb") as fp:
    fp.write(body)


def get_json_data():
  with open(__DOWNLOAD_DATA_PATH__) as data_fp:
    return json.load(data_fp)["data"]


def get_current_day_remote_folder_path():
  return date.today().isoformat()


def download_data_files():
  data = get_json_data()
  downloads_folder_path = __DOWNLOAD_FILE_FOLDER_PATH__
  os.makedirs(downloads_folder_path, exist_ok=True)
  for item in data:
    print(f"Saving {item['name']}...", end=" ")
    download_url(item["url"], os.path.join(downloads_folder_path, item["name"]))
    print("Done ✅")


def upload_data_files():
  pc = HandledPyCloud(os.environ['EMAIL'], os.environ['PASSWORD'], endpoint="eapi")
  downloads_folder_path = __DOWNLOAD_FILE_FOLDER_PATH__
  data = get_json_data()
  files = [os.path.join(downloads_folder_path, item["name"]) for item in data]

  # Create dataset data folder if not exists
  res = pc.createhandledfolderifnotexists(parent_folder_id=0, folder_name=__REMOTE_FOLDER_BASE_PATH__)
  remote_base_data_folder_id = res['metadata']['folderid']

  # Create project data folder if not exists
  res = pc.createhandledfolderifnotexists(parent_folder_id=remote_base_data_folder_id, folder_name=__REMOTE_FOLDER_PROJECT_PATH__)
  remote_project_data_folder_id = res['metadata']['folderid']

  # Create today's data folder if not exists
  res = pc.createhandledfolderifnotexists(parent_folder_id=remote_project_data_folder_id, folder_name=get_current_day_remote_folder_path())
  remote_day_data_folder_id = res['metadata']['folderid']

  # Upload today's files
  print(f"Uploading files...", end=" ")
  res = pc.uploadfile(files=files, folderid=remote_day_data_folder_id)
  if 'error' in res:
    print(res['error'])
    exit(res['result'])
  print("Done ✅")


def main():
  download_data_files()
  upload_data_files()

if __name__ == "__main__":
  main()