# Case invest-box
# ------------------`config.py`------------------

Это файл в корневой папке проекта

`db_config` = {   
    "dbname": "xxx",   
    "user": "xxx",     
    "password": "xxx",   
    "host": "xxx",   
    "port": "xxx",    
}

`ARCHIVE_PATH` = "xxx"

`STAGE_SCHEMA_NAME` = "xxx"  
`DIM_MODEL_SCHEMA_NAME` = "xxx"

`MAX_TABLE_ERROR` - в пределах [0..1]

# -------------------------------------------------

`main.py` - Запуск всех скриптов последовательно
---
Запуск из корневой папки проекта 

Пример запуска модуля (вне main) - из корневой папки проекта:  
`python -m stage_former.stage_former`

---

`docker run --name my-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres` - простой способ развернуть postgres локально с помощью docker

Внутренняя таблица источников
| file_path | success | headers | length |
|----------------|----------------|----------------|--|
| ...file1.csv    | True     | True     |251|
| ...file2.csv     | True     | False     |30|
| ...file3.csv     | False     | False     |500|

# Реестр проверок качества (заметки)
- PK - уникальные
- Выход за допустимый диапазон
- Пропуски - null
- Количество строк было и стало (сначала bad_source - т.е таблицу формируем и при вставке меняем если что, а потом уже сравниваем)
- Проверка времени начало < конца

# Примечание
- Подразумевается, что в конце фалйла в конце стоит \n на последней строке, иначе длина неправильно оценивается.
- table_error = (table_length-error_amount)/source_length