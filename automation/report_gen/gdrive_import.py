from configparser import ConfigParser
from io import StringIO
import pydrive2
from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth
import pandas as pd
from typing import Dict, Optional

cfg = ConfigParser()
cfg.read('config.ini')

gdrive_config = cfg['gdrive']

gauth = GoogleAuth()
gauth.LoadCredentialsFile('credentials.json')
drive = GoogleDrive(gauth)


class GDriveFileNotFound(Exception):
    pass


def _find_folder_id(parent_id: str, folder_name: str) -> str:
    folder_list = drive.ListFile({
        'q': f"""mimeType = 'application/vnd.google-apps.folder' 
                     and title = '{folder_name}' 
                     and '{parent_id}' in parents""",
        'maxResults': 1
    }).GetList()
    if not folder_list:
        raise GDriveFileNotFound(f"Could not find folder {folder_name}")

    return folder_list[0]['id']


def _read_as_csv(file: pydrive2.files.GoogleDriveFile) -> pd.DataFrame:
    contents = file.GetContentString(mimetype='text/csv', encoding='utf-8')
    stringio = StringIO(contents)
    return pd.read_csv(stringio)


def retrieve_teaming_files(class_name: str, checkin_num: str) -> Dict[str, Optional[pd.DataFrame]]:
    # find the teaming data folder
    teaming_folder_id = _find_folder_id(gdrive_config['root_folder_id'], "Teaming Data")

    # find class data folder
    class_folder_id = _find_folder_id(teaming_folder_id, class_name)

    class_files_list = drive.ListFile({
        'q': f"""'{class_folder_id}' in parents and trashed = False"""
    }).GetList()
    if not class_files_list:
        raise GDriveFileNotFound(f"Could not find files in folder {class_name}")

    results = {
        "roster": None,
        "question_dictionary": None,
        "raw": None,
    }
    for file in class_files_list:
        if not file['title'].endswith(".csv"):
            continue
        if f"ROSTER_{checkin_num}" in file['title']:
            results['roster'] = _read_as_csv(file)
        elif f"QUESTION_DICTIONARY_{checkin_num}" in file['title']:
            results['question_dictionary'] = _read_as_csv(file)
        elif f"CHECKIN{checkin_num}_RAW" in file['title']:
            results['raw'] = _read_as_csv(file)

    return results
