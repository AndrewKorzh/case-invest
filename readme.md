# Case

Файл в корневой папке проекта
`config.py:`

`db_config` = {   
    "dbname": "xxx",   
    "user": "xxx",     
    "password": "xxx",   
    "host": "xxx",   
    "port": "xxx",    
}

`ARCHIVE_PATH` = "xxx"

`STAGE_SCHEMA_NAME` = "xxx"

`stage_filler.py` - заполнение данными
`stage_former.py` - создание всех таблиц
---
Запуск из корневой папки проекта 

модули:
python -m stage_former.stage_former


---

`docker run --name my-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres` - простой способ развернуть postgres локально с помощью docker
# Этапы  
## Создание stage (stage_former.py)
- Запускать перед каждой загрузкой даннх (полностью пересоздаёт схему). Иначе - ошибка из-за различия типов данных.
- Все поля текстовые 

## Заполнение данными (stage_filler.py)
- Проверяется структура файлов

| file_path | success | headers | length |
|----------------|----------------|----------------|--|
| ...file1.csv    | True     | True     |251|
| ...file2.csv     | True     | False     |30|
| ...file3.csv     | False     | False     |500|


- Copy в stage успешных файлов и в bad_source не успешных
- Обработка данных (реестр проверок)
- Изменение типов столбцов


# Реестр проверок качества
- PK - уникальные
- Выход за допустимый диапазон
- Пропуски - null
- Количество строк было и стало (сначала bad_source - т.е таблицу формируем и при вставке меняем если что, а потом уже сравниваем)
- Проверка времени начало < конца

# TODO
- После загрузки и обработки - проверка качества -> ETL2 + sucsess / BREAK

# Примечание
- Подразумевается, что в конце фалйла в конце стоит \n даже на последней строке