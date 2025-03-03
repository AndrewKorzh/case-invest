import os
import glob
import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
import config

from data_structure import DATA_STRUCTURE
# print(DATA_STRUCTURE)

def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list


class StageFormer:
    def __init__(self, archive_path, data_structure: dict):
        self.archive_path = archive_path
        self.data_structure = data_structure

    def get_all_dir_files(self, directory_path, extention = "csv"):
        file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
        return file_paths
    
    def process_data(self):
        for key in self.data_structure.keys():
            self.process_table(key)

    def process_file(self, file_path, required_headers):
        file_headers = read_headers(file_path=file_path)
        if len(file_headers) != len(required_headers):
            return [file_path, False, False]
        has_headers = file_headers == required_headers
        return [file_path, True, has_headers]

    def process_table(self, key):
        dir_name = self.data_structure[key]["dir_name"]
        required_headers = list(self.data_structure[key]["headers"].keys())
        print(f"{dir_name}:")
        print(required_headers)
        full_path = os.path.join(self.archive_path, dir_name)
        all_files = self.get_all_dir_files(full_path)
        table = pd.DataFrame(columns=["file_path", "success", "headers"])
        for i, file_path in enumerate(all_files):
            table.loc[i] = self.process_file(file_path=file_path, required_headers=required_headers)
        print(table)
        print("BAD DATA:")
        failed_files = table[table["success"] == False]["file_path"]
        if failed_files.empty:
            print("None")
        else:
            print(failed_files)




if __name__ == "__main__":
    archive_path = config.ARCHIVE_PATH
    data_structure = DATA_STRUCTURE
    sf = StageFormer(archive_path=archive_path, data_structure=data_structure)
    sf.process_data()
