import os
import glob
import sys
from pathlib import Path
import pandas as pd

from data_structure import DATA_STRUCTURE
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, ARCHIVE_PATH, STAGE_SCHEMA_NAME


TEMP_TABLE_NAME = "temp_table"

def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list


class StageFiller:
    def __init__(self):
        self.db_handler = DBHandler()

    def get_all_dir_files(self, directory_path, extention = "csv"):
        file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
        return file_paths
    
    def process_data(self):
        for key in DATA_STRUCTURE.keys():
            print(f"process {key}...")
            self.process_table(key)

    def process_file(self, file_path, required_headers):
        file_headers = read_headers(file_path=file_path)
        if len(file_headers) != len(required_headers):
            return [file_path, False, False]
        has_headers = file_headers == required_headers
        return [file_path, True, has_headers]
    
    def move_from_temp_to_stage(self, table_info,):
        
        return

    def process_table(self, key):
        dir_name = DATA_STRUCTURE[key]["dir_name"]
        required_headers = list(DATA_STRUCTURE[key]["headers"].keys())
        print(f"{dir_name}:")
        print(required_headers)
        full_path = os.path.join(ARCHIVE_PATH, dir_name)
        all_files = self.get_all_dir_files(full_path)
        table = pd.DataFrame(columns=["file_path", "success", "headers"])
        for i, file_path in enumerate(all_files):
            table.loc[i] = self.process_file(file_path=file_path, required_headers=required_headers)
        print("BAD DATA:")
        failed_files = table[table["success"] == False]["file_path"]
        if failed_files.empty:
            print("None")
        else:
            print(failed_files)

        # обработка, если все файлы плохие...

        self.db_handler.create_temp_table(
            table_name=TEMP_TABLE_NAME,
            schema_name=STAGE_SCHEMA_NAME,
            headers=required_headers
        )
        
        for index, row in table.iterrows():
            if row["success"] == True:
                print(row["file_path"])
                self.db_handler.copy_data(
                    table_name=TEMP_TABLE_NAME,
                    schema_name=STAGE_SCHEMA_NAME,
                    file_path=row["file_path"],
                    headers=required_headers,
                    has_headers=row["headers"]
                )


if __name__ == "__main__":
    sf = StageFiller()
    sf.process_data()
