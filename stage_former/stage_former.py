import os
import glob
import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
import config
from data_structure import DATA_STRUCTURE
from db_handler import DBHandler

TEMP_TABLE_NAME = "temp_table"

def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list


class StageFormer:
    def __init__(self, archive_path: str,
                 data_structure: dict,
                 db_config:dict,
                 stage_schema_name: str,
                 ):
        self.archive_path = archive_path
        self.data_structure = data_structure
        self.db_handler = DBHandler(db_config=db_config)
        self.stage_schema_name = stage_schema_name

    def get_all_dir_files(self, directory_path, extention = "csv"):
        file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
        return file_paths
    
    def process_data(self):
        for key in self.data_structure.keys():
            print(f"process {key}...")
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
        print("BAD DATA:")
        failed_files = table[table["success"] == False]["file_path"]
        if failed_files.empty:
            print("None")
        else:
            print(failed_files)

        # обработка, если все файлы плохие...
        self.create_temp_table(columns=required_headers)
        for index, row in table.iterrows():
            if row["success"] == True:
                print(row["file_path"])
                print(row["success"])
                print(row["headers"])
                self.db_handler.copy_data(
                    table_name=TEMP_TABLE_NAME,
                    schema_name=self.stage_schema_name,
                    file_path=row["file_path"],
                    headers=required_headers,
                    has_headers=row["headers"]
                )



# Перенести в db_handler
    def create_temp_table(self, columns: list):
        self.db_handler.execute_query(f"DROP TABLE IF EXISTS {self.stage_schema_name}.{TEMP_TABLE_NAME};")
        query = f"""create table {self.stage_schema_name}.{TEMP_TABLE_NAME}(\n{",\n".join([f"{c} text" for c in columns])});
        """
        print(query)
        self.db_handler.execute_query(query=query)
        




if __name__ == "__main__":
    archive_path = config.ARCHIVE_PATH
    db_config = config.db_config
    stage_schema_name = config.STAGE_SCHEMA_NAME
    data_structure = DATA_STRUCTURE
    sf = StageFormer(archive_path=archive_path,
                     data_structure=data_structure,
                     db_config=db_config,
                     stage_schema_name=stage_schema_name)
    sf.process_data()
