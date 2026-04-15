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

    def get_auth_token(self):
        """Override to log the raw pCloud response for debugging."""
        import subprocess
        from hashlib import sha1

        digest = self.getdigest()
        passworddigest = sha1(
            self.password + bytes(sha1(self.username).hexdigest(), "utf-8") + digest
        )
        params = {
            "getauth": 1,
            "logout": 1,
            "username": self.username.decode("utf-8"),
            "digest": digest.decode("utf-8"),
            "passworddigest": passworddigest.hexdigest(),
            "authexpire": self.token_expire,
        }
        # --- DEBUG: what changed between Apr 12 and Apr 13? ---
        # 1. Runner external IP (did GitHub rotate to a blocked range?)
        try:
            ip = subprocess.run(
                ["curl", "-s", "https://ifconfig.me"],
                capture_output=True, text=True, timeout=5
            ).stdout.strip()
            print(f"[DEBUG] Runner external IP: {ip}")
        except Exception as e:
            print(f"[DEBUG] Could not get IP: {e}")
        # 2. Is getdigest returning a valid nonce?
        print(f"[DEBUG] digest nonce: {digest}")
        # 3. HTTP response headers from pCloud (rate limiting, geo, etc.)
        import requests as req
        raw_resp = req.get(self.endpoint + "userinfo", params=params)
        print(f"[DEBUG] HTTP status: {raw_resp.status_code}")
        print(f"[DEBUG] Response headers: {dict(raw_resp.headers)}")
        # --- END DEBUG ---
        resp = raw_resp.json()
        print(f"[DEBUG] 'auth' present: {'auth' in resp}")
        print(f"[DEBUG] 'result': {resp.get('result')}")
        if "auth" not in resp:
            raise Exception(
                f"pCloud auth failed: 'auth' key missing. "
                f"result={resp.get('result')}, keys={list(resp.keys())}"
            )
        return resp["auth"]

    def createhandledfolderifnotexists(self, parent_folder_id, folder_name):
        res = super().createfolderifnotexists(
            folderid=parent_folder_id, name=folder_name
        )
        if "error" in res:
            print(res["error"])
            exit(res["result"])
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
    pc = HandledPyCloud(os.environ["EMAIL"], os.environ["PASSWORD"], endpoint="eapi")
    downloads_folder_path = __DOWNLOAD_FILE_FOLDER_PATH__
    data = get_json_data()
    files = [os.path.join(downloads_folder_path, item["name"]) for item in data]

    # Create dataset data folder if not exists
    res = pc.createhandledfolderifnotexists(
        parent_folder_id=0, folder_name=__REMOTE_FOLDER_BASE_PATH__
    )
    remote_base_data_folder_id = res["metadata"]["folderid"]

    # Create project data folder if not exists
    res = pc.createhandledfolderifnotexists(
        parent_folder_id=remote_base_data_folder_id,
        folder_name=__REMOTE_FOLDER_PROJECT_PATH__,
    )
    remote_project_data_folder_id = res["metadata"]["folderid"]

    # Create today's data folder if not exists
    res = pc.createhandledfolderifnotexists(
        parent_folder_id=remote_project_data_folder_id,
        folder_name=get_current_day_remote_folder_path(),
    )
    remote_day_data_folder_id = res["metadata"]["folderid"]

    # Upload today's files
    print(f"Uploading files...", end=" ")
    res = pc.uploadfile(files=files, folderid=remote_day_data_folder_id)
    if "error" in res:
        print(res["error"])
        exit(res["result"])
    print("Done ✅")


def main():
    download_data_files()
    upload_data_files()


if __name__ == "__main__":
    main()
