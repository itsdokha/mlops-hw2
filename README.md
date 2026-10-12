# ДЗ №2: Kafka и ClickHouse

CSV с транзакциями загружается в топик Kafka. ClickHouse читает топик через Kafka Engine
и через Materialized View сохраняет данные в таблицы MergeTree. Потом по ним считается
категория наибольшей транзакции в каждом штате.

Таблица `hw.kafka_transactions` (Kafka Engine) читает топик `transactions`. Два materialized view
перекладывают данные из неё в две таблицы: `hw.transactions_basic` (базовая схема, п.2)
и `hw.transactions` (оптимизированная схема, п.4).

## Структура

| Файл | Что внутри | Пункт |
|---|---|---|
| `producer/producer.py` | загрузка CSV в топик Kafka (JSON на каждую строку) | 1 |
| `sql/01_ddl.sql` | DDL: Kafka Engine таблица, базовая MergeTree таблица, MV | 2 |
| `sql/03_query.sql` | запрос: категория наибольшей транзакции по каждому штату | 3 |
| `results/result.csv` | результат запроса | 3 |
| `sql/02_ddl_optimized.sql` | оптимизированный DDL (типы, кодеки, ORDER BY, партиции, проекция) | 4 |
| `sql/04_storage_stats.sql`, `sql/05_benchmark.sql` | сравнение размера и скорости двух таблиц | 4 |
| `docker-compose.yml` | Kafka (KRaft), ClickHouse, producer | |
| `requirements.txt` | зависимости producer | |

DDL из `sql/01_ddl.sql` и `sql/02_ddl_optimized.sql` выполняется автоматически при первом
старте ClickHouse (`/docker-entrypoint-initdb.d`).

## Запуск

Нужны Docker и Docker Compose. Для Compose v1 вместо `docker compose` использовать `docker-compose`.

1. Скачать данные с [Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394)
   и положить `train.csv` в папку `data/`:

   ```bash
   mkdir -p data
   unzip teta-ml-1-2025.zip train.csv -d data
   ```

2. Поднять контейнеры:

   ```bash
   docker compose up -d --build
   ```

   Kafka и ClickHouse стартуют, ClickHouse создаёт таблицы, затем контейнер `hw2-producer`
   отправляет все строки `train.csv` в топик и завершается. Это занимает около 10 секунд:

   ```bash
   docker logs hw2-producer
   # Done: 786431 messages sent to 'transactions' in 7.1s, errors: 0
   ```

3. Проверить, что данные дошли до ClickHouse (обе таблицы по 786431 строке):

   ```bash
   docker exec hw2-clickhouse clickhouse-client -q \
     "SELECT 'basic', count() FROM hw.transactions_basic UNION ALL SELECT 'optimized', count() FROM hw.transactions"
   ```

4. Выполнить запрос и сохранить результат в CSV:

   ```bash
   docker exec hw2-clickhouse clickhouse-client --queries-file /sql/03_query.sql \
     --format CSVWithNames > results/result.csv
   ```

5. Сравнить базовую и оптимизированную схемы:

   ```bash
   docker exec hw2-clickhouse clickhouse-client -q "OPTIMIZE TABLE hw.transactions FINAL"
   docker exec hw2-clickhouse clickhouse-client -q "OPTIMIZE TABLE hw.transactions_basic FINAL"
   docker exec hw2-clickhouse clickhouse-client --queries-file /sql/04_storage_stats.sql --format PrettyCompact
   docker exec hw2-clickhouse clickhouse-client --queries-file /sql/05_benchmark.sql
   ```

6. Остановить контейнеры:

   ```bash
   docker compose down
   ```

Producer можно запустить и с хоста (Kafka доступна на `localhost:9092`):

```bash
pip install -r requirements.txt
python producer/producer.py --bootstrap-servers localhost:9092 --file data/train.csv
```

Повторный запуск дублирует строки в таблицах. Чтобы начать с чистого листа, нужно выполнить
`docker compose down && docker compose up -d`.

## Результат (п.3)

Запрос (`sql/03_query.sql`):

```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount)            AS max_amount
FROM hw.transactions
GROUP BY us_state
ORDER BY us_state;
```

`argMax(cat_id, amount)` возвращает категорию строки с максимальной суммой внутри каждого штата.
Результат для 51 штата лежит в [`results/result.csv`](results/result.csv).

## Оптимизация (п.4)

Изменения в `hw.transactions` по сравнению с `hw.transactions_basic`:

- **`LowCardinality(String)`** для колонок с малым числом уникальных значений: `cat_id` (14),
  `us_state` (51), `gender` (2), `merch` (~700), `jobs`, `one_city`, `street`, имена (до ~1000).
  Строки хранятся как словарь и индексы, поэтому группировка по ним заметно быстрее.
- **Компактные типы**: `Decimal(10, 2)` для суммы (точные деньги без погрешности float),
  `UInt8` / `UInt32` вместо `Int64`, `Float32` для координат клиента, `post_code` как `UInt32`.
- **Кодеки**: `Delta + ZSTD` для времени, `Gorilla + ZSTD` для координат, `ZSTD` для чисел.
- **`ORDER BY (us_state, cat_id, transaction_time)`**: данные одного штата лежат подряд.
  Фильтры по штату и категории пропускают лишние гранулы, а похожие значения рядом лучше сжимаются.
- **`PARTITION BY toYYYYMM(transaction_time)`**: запросы по периоду отбрасывают лишние партиции.
- **`PROJECTION p_state_max_amount`**: агрегат `argMax(cat_id, amount)` / `max(amount)` по штатам
  считается при вставке. Запрос из п.3 автоматически читает проекцию, а не всю таблицу.

Замеры на 786 431 строке (после `OPTIMIZE ... FINAL`):

| | `transactions_basic` | `transactions` |
|---|---|---|
| Размер на диске (сжатый) | 54.6 MiB | 27.1 MiB |
| Размер без сжатия | 125.2 MiB | 43.2 MiB |
| Прочитано строк запросом п.3 | 786 431 | 751 |
| Прочитано байт запросом п.3 | 28.9 MiB | 82.9 KiB |
| Время запроса п.3 | ~11 ms | ~3 ms |
