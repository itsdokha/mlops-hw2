-- Сравнение размера хранения базовой и оптимизированной таблиц.
SELECT
    table,
    sum(rows)                                          AS rows,
    formatReadableSize(sum(data_compressed_bytes))     AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))   AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.parts
WHERE database = 'hw' AND active AND table IN ('transactions_basic', 'transactions')
GROUP BY table
ORDER BY table;
