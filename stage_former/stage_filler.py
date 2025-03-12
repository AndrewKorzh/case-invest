import os
import glob
import sys
from pathlib import Path
import pandas as pd
import time

from stage_tables_info import TABLES_INFO, SERVICE_TABLES, ERROR_LOG_TABLE_NAME, BAD_SOURCE_TABLE_NAME, DATA_UPDATE_TABLE_NAME, ROW_COUNT_COMPARISON
from inspections_register import INSPECTIONS_REGISTER
from stage_db_handler import DBHandler

sys.path.append(str(Path(__file__).parent.parent))
from config import ARCHIVE_PATH, STAGE_SCHEMA_NAME, DIM_MODEL_SCHEMA_NAME
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
    if has_headers:
        lines_amt -= 1
    return [file_path, True, has_headers, lines_amt]

def get_all_files_in_dirrectory(directory_path, extention = "csv"):
    file_paths = glob.glob(os.path.join(directory_path, f"*.{extention}"))
    return file_paths

class StageFiller:
    def __init__(self):
        self.db_handler = DBHandler()

    def fill_all(self, clean_before_insert = True):
        logger.log("Filling Tables\n")
        if clean_before_insert:
            self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=BAD_SOURCE_TABLE_NAME)
            self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=ERROR_LOG_TABLE_NAME)
            self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=ROW_COUNT_COMPARISON)
        for table_info in TABLES_INFO:
            logger.log(f"{table_info["table_name"]}...")
            self.fill_table(table_info, clean_before_insert=clean_before_insert)

    def fill_table(self, table_info:dict, clean_before_insert = True):
        dir_name = table_info["dir_name"]
        table_name = table_info["table_name"]
        required_headers = list(table_info["headers"].keys())
        full_path = os.path.join(ARCHIVE_PATH, dir_name)

        all_files = get_all_files_in_dirrectory(full_path)
        table = pd.DataFrame(columns=["file_path", "success", "headers", "lines_amt"])
        for i, file_path in enumerate(all_files):
            table.loc[i] = check_file(file_path=file_path, required_headers=required_headers)

        if clean_before_insert:
            self.db_handler.clear_table(schema_name=STAGE_SCHEMA_NAME,table_name=table_name)
        
        for index, row in table.iterrows():
            file_success = row["success"]
            if file_success == True:
                logger.log(f"{table_name} - {index+1}/{table.shape[0]}", r033=True)
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
                table.at[index, 'success'] = False


        logger.log("")
        logger.log(f"\n\n\n{table}")
        successed_files_lines = table.loc[table['success'] == True, 'lines_amt'].sum()
        db_table_length = self.db_handler.count_lines_amount(schema_name=STAGE_SCHEMA_NAME, table_name=table_name)
        self.db_handler.insert_row_count_comparison(schema_name=STAGE_SCHEMA_NAME,
                                                    row_count_comparison_table_name=ROW_COUNT_COMPARISON,
                                                    table_name = table_name,
                                                    source_length=successed_files_lines,
                                                    db_table_length=db_table_length
                                                    )
        logger.log(f"db: {db_table_length}")
        logger.log(f"files: {successed_files_lines}\n\n\n")


        

    def data_quality_check(self):
        # Очистка таблиц некоторых (чтобы не скапливалось ничего)
        # Заполнение данными 
        # Расчёт 
        # Отедельно расчёт показателей
        for table_inspections in INSPECTIONS_REGISTER:
            logger.log(f"process {table_inspections["table_name"]}...")
            self.process_table(table_inspections)

        for table in SERVICE_TABLES:
            print(f"{table["table_name"]} coppy")
            self.db_handler.copy_table(schema_from=STAGE_SCHEMA_NAME, schema_to=DIM_MODEL_SCHEMA_NAME, table_name=table["table_name"])
            


    # Применение проверки ckeck_applying
    def process_table(self, table_inspections:dict):
        table_name = table_inspections["table_name"]
        primary_key = table_inspections["primary_key"]
        inspections = table_inspections["inspections"]

        for i in inspections:
            inspection_type = i["type"]
            if inspection_type == "DUPLICATE":
                self.db_handler.process_unique(
                    schema_name=STAGE_SCHEMA_NAME,
                    table_name=table_name,
                    table_primary_key=primary_key,
                    column_name=i["column"],
                    error_type = "DUPLICATE",
                    error_table=ERROR_LOG_TABLE_NAME
                )
            # > - все которые больше, < - меньше
            if inspection_type == "MAXIMUM_VALUE_EXCEEDED":
                self.db_handler.process_bound_value(
                    schema_name=STAGE_SCHEMA_NAME,
                    table_name=table_name,
                    table_primary_key=primary_key,
                    column_name=i["column"],
                    error_type = "MAXIMUM_VALUE_EXCEEDED",
                    value=i["value"],
                    sign=">",
                    error_table=ERROR_LOG_TABLE_NAME
                )
                # 
            if inspection_type == "BELOW_MINIMUM_THRESHOLD":
                self.db_handler.process_bound_value(
                    schema_name=STAGE_SCHEMA_NAME,
                    table_name=table_name,
                    table_primary_key=primary_key,
                    column_name=i["column"],
                    error_type = "BELOW_MINIMUM_THRESHOLD",
                    value=i["value"],
                    sign="<",
                    error_table=ERROR_LOG_TABLE_NAME
                )


if __name__ == "__main__":
    sf = StageFiller()

    start_time = time.perf_counter()
    sf.fill_all()
    elapsed_time = time.perf_counter() - start_time
    logger.log(f"Время, ушедшее на заполнение таблиц: {elapsed_time:.2f} секунд")

    # Обработка
    start_time = time.perf_counter()
    sf.data_quality_check()
    elapsed_time = time.perf_counter() - start_time
    logger.log(f"Время, ушедшее на обработку таблиц: {elapsed_time:.2f} секунд")


    # # Проверка данных
    # start_time = time.perf_counter()
    # sf.check_data_loss()
    # elapsed_time = time.perf_counter() - start_time
    # logger.log(f"Время, ушедшее на проверку данных: {elapsed_time:.2f} секунд")


