-- Сравнение выполнения запроса из 03_query.sql на базовой и оптимизированной таблицах.
SELECT us_state, argMax(cat_id, amount) AS category, max(amount) AS max_amount
FROM hw.transactions_basic GROUP BY us_state ORDER BY us_state
FORMAT Null SETTINGS log_comment = 'bench_transactions_basic';

SELECT us_state, argMax(cat_id, amount) AS category, max(amount) AS max_amount
FROM hw.transactions GROUP BY us_state ORDER BY us_state
FORMAT Null SETTINGS log_comment = 'bench_transactions';

SYSTEM FLUSH LOGS;

SELECT
    log_comment                    AS run,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    query_duration_ms,
    projections
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('bench_transactions_basic', 'bench_transactions')
ORDER BY event_time_microseconds DESC
LIMIT 2
FORMAT PrettyCompact;
