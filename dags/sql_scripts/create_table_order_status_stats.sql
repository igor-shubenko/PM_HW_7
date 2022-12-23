-- creation of order_status_stats table
CREATE TABLE IF NOT EXISTS order_status_stats(
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP NOT NULL,
    order_status_name VARCHAR(100) NOT NULL,
    orders_count INT NOT NULL
);