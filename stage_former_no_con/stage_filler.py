import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from tables_info import TABLES_INFO
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import ARCHIVE_PATH, STAGE_SCHEMA_NAME

def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list


def check_file(file_path, required_headers):
    file_headers = read_headers(file_path=file_path)
    if len(file_headers) != len(required_headers):
        return [file_path, False, False]
    has_headers = file_headers == required_headers
    return [file_path, True, has_headers]

def get_all_files_in_dirrectory(directory_path, extention = "csv"):
    file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
    return file_paths

class StageFiller:
    def __init__(self):
        self.db_handler = DBHandler()

    def fill_all(self):
        for table_info in TABLES_INFO:
            print(f"filling {table_info["table_name"]}...")
            self.fill_table(table_info)

    def fill_table(self, table_info:dict):
        dir_name = table_info["dir_name"]
        table_name = table_info["table_name"]
        required_headers = list(table_info["headers"].keys())
        full_path = os.path.join(ARCHIVE_PATH, dir_name)

        all_files = get_all_files_in_dirrectory(full_path)
        table = pd.DataFrame(columns=["file_path", "success", "headers"])
        for i, file_path in enumerate(all_files):
            table.loc[i] = check_file(file_path=file_path, required_headers=required_headers)
        failed_files = table[table["success"] == False]["file_path"]

        # Обработка плохих файлов - сделать нормально (в табличку) - можно в table поместить число записей + переименовать
        if failed_files.empty:
            print("All files ok")
        else:
            print(f"Failed: {failed_files}")
        
        for index, row in table.iterrows():
            if row["success"] == True:
                print(f"{table_name} - {index+1}/{table.shape[0]}")
                self.db_handler.copy_data(
                    table_name=table_name,
                    schema_name=STAGE_SCHEMA_NAME,
                    file_path=row["file_path"],
                    headers=required_headers,
                    has_headers=row["headers"]
                )



if __name__ == "__main__":
    start_time = time.perf_counter()
    sf = StageFiller()
    sf.fill_all()
    elapsed_time = time.perf_counter() - start_time
    print(f"Время, ушедшее на заполнение таюлиц: {elapsed_time:.2f} секунд")
    # Теперь нужна обработка
