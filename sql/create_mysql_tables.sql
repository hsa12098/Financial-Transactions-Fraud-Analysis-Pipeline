-- sql/create_mysql_tables.sql
-- Drop tables if they exist
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS transaction_summary;

CREATE TABLE IF NOT EXISTS transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_time DOUBLE,
    transaction_amount DOUBLE,
    is_fraud INT,
    data_source VARCHAR(50),
    processed_timestamp TIMESTAMP,
    processed_hour INT,
    processed_day INT,
    avg_amount_by_source DOUBLE,
    amount_to_avg_ratio DOUBLE,
    fraud_score DOUBLE
);

CREATE TABLE IF NOT EXISTS transaction_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    is_fraud INT,
    data_source VARCHAR(50),
    total_amount DOUBLE,
    transaction_count BIGINT,
    avg_fraud_score DOUBLE,
    batch_id BIGINT,
    processed_timestamp TIMESTAMP
);