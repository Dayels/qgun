# qgun

Утилита для выполнения SQL-запросов к нескольким базам данных (шардам) параллельно или последовательно с выводом результатов в табличном или CSV формате.

На данный момент поддерживается только PostgreSQL [см. Формат строки подключения](https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters)

## Использование

`qgun [FLAGS] -q SQL_QUERY [ARGS]`
`qgun [FLAGS] -f SQL_FILE [ARGS]`

Обязательные параметры: 

- `-c` - строки подключения к БД (через запятую) либо через переменную окружения DB_CONN_STRINGS
- либо `-f SQL_FILE`, либо `-q SQL_QUERY`

### Примеры использования

Выполнить запрос из файла к двум базам данных параллельно, вывод в CSV:

```bash
./qgun -c 'host=localhost port=5432 user=user1 password=pass1 dbname=db1,host=localhost port=5432 user=user2 password=pass2 dbname=db2' -f query.sql -p -o csv
```

С использованием переменных окружения:

```bash
export DB_CONN_STRINGS='host=localhost dbname=test,host=remote dbname=prod'
export SQL_QUERY='SELECT count(*) FROM users'
./qgun -p
```

С использованием параметров в запросе и таймаутом

```bash
# параметры программы и будут параметрами SQL-запроса
./qgun -t 10s -c 'user=user1 password=pass1 dbname=db1' -q 'SELECT count(*) FROM users WHERE name = $1' James
```

### Все параметры командной строки:

```
-c string
      Строки подключения к БД (через запятую) (env DB_CONN_STRINGS)
-f string
      Имя файла с SQL-запросом (env SQL_FILE)
-q string
      SQL-запрос (env SQL_QUERY)
-t duration
      Таймаут выполнения запроса (env QUERY_TIMEOUT) (default 5s)
-l int
      Ограничение количества возвращаемых строк (0 - без ограничения) (env MAX_ROWS) (default 200)
-o string
      Формат вывода возвращаемого результата [table, csv] (env OUTPUT_FORMAT) (default "table")
-p    Параллельное выполнение запросов (env PARALLEL_MODE) (default false)
-v    Включение debug логов (default false)
```

### Переменные окружения:

Все параметры можно также задать через переменные окружения:

- DB_CONN_STRINGS - строки подключения к БД
- SQL_FILE - файл с SQL-запросом
- SQL_QUERY - SQL-запрос
- QUERY_TIMEOUT - таймаут выполнения
- MAX_ROWS - ограничение количества строк
- OUTPUT_FORMAT - формат вывода
- PARALLEL_MODE - параллельный режим

## Форматы вывода

- table - табличный формат с выравниванием столбцов
- csv - данные в формате CSV с разделителем-запятой

## Особенности работы

- При ошибке в любом из подключений утилита завершится с ненулевым кодом возврата
- В режиме параллельного выполнения порядок вывода результатов может быть произвольным
- Вне зависимоти от выбора формата вывода каждая строка результата дополняется признаком номера коннекта по порядку начиная с DB_0
- Утилита предназначена в первую очередь для SELECT-запросов, но позволяет выполнять любой запрос выполнение которого предпологает возврат строк
- Заданый напрямую SQL запрос в приоритетнее файла с запросом