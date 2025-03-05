import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from tables_info import TABLES_INFO, ERROR_LOG_TABLE_NAME, BAD_SOURCE_TABLE_NAME
from inspections_register import INSPECTIONS_REGISTER
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import ARCHIVE_PATH, STAGE_SCHEMA_NAME

def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list

def count_lines(file_path, chunk_size=1024 * 1024):
    count = 0
    with open(file_path, 'rb') as f:  # Открываем в бинарном режиме (быстрее)
        while chunk := f.read(chunk_size):  # Читаем файл блоками (чанками)
            count += chunk.count(b'\n')  # Считаем количество `\n` в чанке
    return count

def check_file(file_path, required_headers):
    file_headers = read_headers(file_path=file_path)
    lines_amt = count_lines(file_path)
    if len(file_headers) != len(required_headers):
        return [file_path, False, False, lines_amt]
    has_headers = file_headers == required_headers
    return [file_path, True, has_headers, lines_amt]

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
        table = pd.DataFrame(columns=["file_path", "success", "headers", "lines_amt"])
        for i, file_path in enumerate(all_files):
            table.loc[i] = check_file(file_path=file_path, required_headers=required_headers)

        
        failed_pairs = table.loc[~table["success"], ["file_path", "lines_amt"]].values.tolist()

        
        for pair in failed_pairs:
            print(pair)
            self.db_handler.bad_source(schema_name=STAGE_SCHEMA_NAME,
                                       table_name=table_name,
                                       bad_source_table_name=BAD_SOURCE_TABLE_NAME,
                                       source=pair[0],
                                       length=pair[1])
            
        self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=table_name)
        
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

    def process_all(self):
        for table_inspections in INSPECTIONS_REGISTER:
            print(f"process {table_inspections["table_name"]}...")
            self.process_table(table_inspections)

    def process_table(self, table_inspections:dict):
        table_name = table_inspections["table_name"]
        primary_key = table_inspections["primary_key"]
        inspections = table_inspections["inspections"]

        for i in inspections:
            inspection_type = i["type"]
            if inspection_type == "UNIQUE":
                self.db_handler.process_unique(
                    schema_name=STAGE_SCHEMA_NAME,
                    table_name=table_name,
                    table_primary_key=primary_key,
                    column_name=i["collumn"],
                    error_table=ERROR_LOG_TABLE_NAME
                )

        print(f"{table_name} processed")




if __name__ == "__main__":
    sf = StageFiller()

    start_time = time.perf_counter()
    sf.fill_all()
    elapsed_time = time.perf_counter() - start_time
    print(f"Время, ушедшее на заполнение таблиц: {elapsed_time:.2f} секунд")

    # Тестовое заполненеие кривыми данными
    # sf.fill_table(table_info=TABLES_INFO[0]) # это product_type

    # Обработка
    start_time = time.perf_counter()
    sf.process_all()
    elapsed_time = time.perf_counter() - start_time
    print(f"Время, ушедшее на очистку таюлиц: {elapsed_time:.2f} секунд")

    # Изменение типов


# ALTER TABLE your_table 
# ALTER COLUMN created_at TYPE DATE USING TO_DATE(created_at, 'DD/MM/YYYY');

# ALTER TABLE your_table 
# ALTER COLUMN product_type_cd TYPE INT USING NULLIF(product_type_cd, '')::INTEGER;