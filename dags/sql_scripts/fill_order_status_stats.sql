--clean table
TRUNCATE order_status_stats;

-- fill order_status_stats table from sale table
WITH order_stats as (
    SELECT
        date_trunc('day', sale.date_sale) AS date,
        status_name.status_name AS status_name,
        COUNT (status_name.status_name) AS orders_count
    FROM sale
        JOIN order_status ON sale.sale_id=order_status.sale_id
        JOIN status_name ON order_status.status_name_id=status_name.status_name_id
    GROUP BY
        sale.date_sale, status_name.status_name
    ORDER BY
        1 DESC, 2
)

INSERT INTO order_status_stats(dt, order_status_name, orders_count) SELECT * FROM order_stats;

