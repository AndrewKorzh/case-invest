import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from tables_info import TABLES_INFO, ERROR_LOG_TABLE_NAME, BAD_SOURCE_TABLE_NAME, DATA_UPDATE_TABLE_NAME
from inspections_register import INSPECTIONS_REGISTER
from db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import ARCHIVE_PATH, STAGE_SCHEMA_NAME
from logger import logger


def read_headers(file_path) -> list:
    with open(file_path, 'r') as f:
        headers = f.readline()
    headers_list = [header.strip() for header in headers.split(",")]
    return headers_list

def count_lines(file_path, chunk_size=1024 * 1024):
    count = 0
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            count += chunk.count(b'\n')
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
        logger.log("filling Tables\n")
        for table_info in TABLES_INFO:
            logger.log(f"Filling {table_info["table_name"]}...")
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

        self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=table_name)
        
        for index, row in table.iterrows():
            file_success = row["success"]
            if file_success == True:
                # logger.log(f"{table_name} - {index+1}/{table.shape[0]}")
                file_success = self.db_handler.copy_data(
                    table_name=table_name,
                    schema_name=STAGE_SCHEMA_NAME,
                    file_path=row["file_path"],
                    columns=required_headers,
                    has_headers=row["headers"]
                )
            
            if file_success != True:
                self.db_handler.add_bad_source(schema_name=STAGE_SCHEMA_NAME,
                                            table_name=table_name,
                                            bad_source_table_name=BAD_SOURCE_TABLE_NAME,
                                            source=row["file_path"],
                                            length=row["lines_amt"])

    def process_all(self):
        for table_inspections in INSPECTIONS_REGISTER:
            logger.log(f"process {table_inspections["table_name"]}...")
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

        logger.log(f"{table_name} processed")

    def change_types_all(self):
        for table_info in TABLES_INFO:
            logger.log(f"changing {table_info["table_name"]}...")
            self.change_types_table(table_info)

    def change_types_table(self, table_info):
        table_name = table_info["table_name"]
        headers = table_info["headers"]
        self.db_handler.change_table_types(schema_name=STAGE_SCHEMA_NAME,
                                           table_name=table_name,
                                           headers=headers)
        return
    
    def check_data_loss(self, k = 0.95):
        for table_info in TABLES_INFO:
            table_name, data_loaded, data_los, load_ratio = self.check_data_loss_table(table_info)
            if load_ratio < k:
                query = f"""
                    INSERT INTO {STAGE_SCHEMA_NAME}.{DATA_UPDATE_TABLE_NAME} 
                    (success, message)
                    VALUES 
                    (false, 'load_ratio = {load_ratio} < {k} in {table_name}');
                """
                self.db_handler.execute_query(query)
                return
        
        query = f"""
            INSERT INTO {STAGE_SCHEMA_NAME}.{DATA_UPDATE_TABLE_NAME} 
            (success, message)
            VALUES 
            (TRUE, 'ok');
        """
        self.db_handler.execute_query(query)
    
    def check_data_loss_table(self, table_info):
        table_name = table_info["table_name"]
        data_loaded, data_los = self.db_handler.check_data_loss(schema_name=STAGE_SCHEMA_NAME,
                                           table_name=table_name,)
        
        if data_loaded + data_los != 0:
            load_ratio = data_loaded / (data_loaded + data_los)
        else:
            load_ratio = 0

        return table_name, data_loaded, data_los, load_ratio

            


if __name__ == "__main__":
    sf = StageFiller()

    start_time = time.perf_counter()
    sf.fill_all()
    elapsed_time = time.perf_counter() - start_time
    logger.log(f"Время, ушедшее на заполнение таблиц: {elapsed_time:.2f} секунд")

    # # Обработка
    # start_time = time.perf_counter()
    # sf.process_all()
    # elapsed_time = time.perf_counter() - start_time
    # logger.log(f"Время, ушедшее на очистку таблиц: {elapsed_time:.2f} секунд")

    # # Изменение типов
    # start_time = time.perf_counter()
    # sf.change_types_all()
    # elapsed_time = time.perf_counter() - start_time
    # logger.log(f"Время, ушедшее на изменение типов: {elapsed_time:.2f} секунд")



    # # Проверка данных
    # start_time = time.perf_counter()
    # sf.check_data_loss()
    # elapsed_time = time.perf_counter() - start_time
    # logger.log(f"Время, ушедшее на проверку данных: {elapsed_time:.2f} секунд")


