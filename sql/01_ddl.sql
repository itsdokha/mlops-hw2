-- Пункт 2: таблицы, которые получают данные из топика Kafka.
-- Базовая (неоптимизированная) схема.

CREATE DATABASE IF NOT EXISTS hw;

-- Источник: Kafka Engine читает JSON-сообщения из топика transactions.
-- Все поля читаются как String, приведение типов делается в materialized view.
CREATE TABLE IF NOT EXISTS hw.kafka_transactions
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           String,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              String,
    lon              String,
    population_city  String,
    jobs             String,
    merchant_lat     String,
    merchant_lon     String,
    target           String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536;

-- Хранилище: обычный MergeTree без оптимизаций.
CREATE TABLE IF NOT EXISTS hw.transactions_basic
(
    transaction_time DateTime,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = MergeTree
ORDER BY transaction_time;

-- Перекладывает сообщения из Kafka в MergeTree.
CREATE MATERIALIZED VIEW IF NOT EXISTS hw.mv_transactions_basic TO hw.transactions_basic AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    toFloat64(amount)                         AS amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    toFloat64(lat)                            AS lat,
    toFloat64(lon)                            AS lon,
    toInt64(population_city)                  AS population_city,
    jobs,
    toFloat64(merchant_lat)                   AS merchant_lat,
    toFloat64(merchant_lon)                   AS merchant_lon,
    toInt64(target)                           AS target
FROM hw.kafka_transactions;
