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

## Проверка качества данных
- Проверка на потерянные данные (count(table) / (count(table) + count(error_log.table) + countbad_sourse.length)) > k

# Реестр проверок качества
- PK - уникальные
- limit > 0
- Пропуски - null
- Количество строк было и стало (сначала bad_source - т.е таблицу формируем и при вставке меняем если что, а потом уже сравниваем)

# Проверка качества - в витрины
- Недопустимые значения - зп < 0 и тд

# TODO
- Обернуть всё что можно в try exept
- После загрузки и обработки - проверка качества -> ETL2 + sucsess / BREAK
- Перенести из filler в db_handler sql запросы
- Помимо bad sorce проверять сошлись ли строки - получается расчёт строк во вне 


- у пользователя нет несколько подряд идущих enable enable disable disable enable - удаляем то, что не в том порядке
- tail_limit > 0
- disable = 0
- create < delt
- округление копилки (сопоставление trans с limit)
- тип счёта сопоставлять с заявкой
- файлики
- Преобразование типов


# Примечание
- Подразумевается, что в конце фалйла в конце стоит \n даже на последней строке