-- Пункт 3: категория наибольшей транзакции по каждому штату.
SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount)            AS max_amount
FROM hw.transactions
GROUP BY us_state
ORDER BY us_state;
