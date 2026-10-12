-- Пункт 4: оптимизированная схема хранения (изменённый DDL из п.2).
-- Читает тот же Kafka-источник hw.kafka_transactions через свой materialized view,
-- поэтому обе таблицы наполняются одинаковыми данными и их можно сравнить.
--
-- Что изменено относительно hw.transactions_basic:
--   * LowCardinality(String) для колонок с малым числом уникальных значений
--     (категории, штаты, города, мерчанты и т.д. — от 2 до ~1000 значений);
--   * компактные типы: Decimal(10, 2) для суммы (без погрешности float),
--     UInt8/UInt32 вместо Int64, Float32 для координат клиента;
--   * кодеки сжатия: Delta для времени, Gorilla для координат, ZSTD;
--   * ORDER BY (us_state, cat_id, transaction_time): данные одного штата лежат
--     рядом — фильтры и GROUP BY по штату читают меньше гранул, лучше сжатие;
--   * PARTITION BY toYYYYMM(transaction_time) — отсечение партиций по дате;
--   * PROJECTION с предагрегатом по штатам под основной аналитический запрос:
--     запрос из 03_query.sql читает ~сотни строк вместо всей таблицы.

CREATE TABLE IF NOT EXISTS hw.transactions
(
    transaction_time DateTime                CODEC(Delta, ZSTD(1)),
    merch            LowCardinality(String),
    cat_id           LowCardinality(String),
    amount           Decimal(10, 2)          CODEC(ZSTD(1)),
    name_1           LowCardinality(String),
    name_2           LowCardinality(String),
    gender           LowCardinality(String),
    street           LowCardinality(String),
    one_city         LowCardinality(String),
    us_state         LowCardinality(String),
    post_code        UInt32                  CODEC(ZSTD(1)),
    lat              Float32                 CODEC(Gorilla, ZSTD(1)),
    lon              Float32                 CODEC(Gorilla, ZSTD(1)),
    population_city  UInt32                  CODEC(ZSTD(1)),
    jobs             LowCardinality(String),
    merchant_lat     Float64                 CODEC(Gorilla, ZSTD(1)),
    merchant_lon     Float64                 CODEC(Gorilla, ZSTD(1)),
    target           UInt8,

    PROJECTION p_state_max_amount
    (
        SELECT
            us_state,
            argMax(cat_id, amount),
            max(amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS hw.mv_transactions TO hw.transactions AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    toDecimal64(amount, 2)                    AS amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    toUInt32(post_code)                       AS post_code,
    toFloat32(lat)                            AS lat,
    toFloat32(lon)                            AS lon,
    toUInt32(population_city)                 AS population_city,
    jobs,
    toFloat64(merchant_lat)                   AS merchant_lat,
    toFloat64(merchant_lon)                   AS merchant_lon,
    toUInt8(target)                           AS target
FROM hw.kafka_transactions;
