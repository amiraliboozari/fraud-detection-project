CREATE DATABASE fraud_detection;
USE fraud_detection;

CREATE TABLE transactions (
    step INT,
    type VARCHAR(20),
    amount DECIMAL(12,2),
    nameOrig VARCHAR(50),
    oldbalanceOrg DECIMAL(12,2),
    newbalanceOrig DECIMAL(12,2),
    nameDest VARCHAR(50),
    oldbalanceDest DECIMAL(12,2),
    newbalanceDest DECIMAL(12,2),
    isFraud TINYINT,
    isFlaggedFraud TINYINT
);

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'C:/Users/Amir/Desktop/AIML Dataset.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
 nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud);

SELECT COUNT(*) FROM transactions;

SELECT * 
FROM transactions
LIMIT 10;

SELECT COUNT(*) AS total_rows FROM transactions;

-- distinct transaction types
SELECT `type`, COUNT(*) AS cnt
FROM transactions
GROUP BY `type`
ORDER BY cnt DESC;

-- basic fraud counts
SELECT isFraud, COUNT(*) AS num_transactions
FROM transactions
GROUP BY isFraud;

-- null conts per column
SELECT
  SUM(step IS NULL)           AS step_nulls,
  SUM(`type` IS NULL)         AS type_nulls,
  SUM(amount IS NULL)         AS amount_nulls,
  SUM(nameOrig IS NULL)       AS nameOrig_nulls,
  SUM(oldbalanceOrg IS NULL)  AS oldbalanceOrg_nulls,
  SUM(newbalanceOrig IS NULL) AS newbalanceOrig_nulls,
  SUM(nameDest IS NULL)       AS nameDest_nulls,
  SUM(oldbalanceDest IS NULL) AS oldbalanceDest_nulls,
  SUM(newbalanceDest IS NULL) AS newbalanceDest_nulls,
  SUM(isFraud IS NULL)        AS isFraud_nulls,
  SUM(isFlaggedFraud IS NULL) AS isFlaggedFraud_nulls
FROM transactions;

-- adding surrogate key
ALTER TABLE transactions ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY;

-- first 100k rows
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT CONCAT(step,'|',type,'|',amount,'|',nameOrig,'|',nameDest,'|',isFraud)) AS distinct_rows
FROM transactions
WHERE id BETWEEN 1 AND 100000;

-- Second 100k
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT CONCAT(step,'|',type,'|',amount,'|',nameOrig,'|',nameDest,'|',isFraud)) AS distinct_rows
FROM transactions
WHERE id BETWEEN 100001 AND 200000;

-- Third 100k
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT CONCAT(step,'|',type,'|',amount,'|',nameOrig,'|',nameDest,'|',isFraud)) AS distinct_rows
FROM transactions
WHERE id BETWEEN 200001 AND 300000;

-- Fourth 100k
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT CONCAT(step,'|',type,'|',amount,'|',nameOrig,'|',nameDest,'|',isFraud)) AS distinct_rows
FROM transactions
WHERE id BETWEEN 300001 AND 400000;

-- Count negatives
SELECT COUNT(*) AS negative_amounts
FROM transactions
WHERE amount < 0;

-- Count zeros
SELECT COUNT(*) AS zero_amounts
FROM transactions
WHERE amount = 0;

-- Origin inconsistencies
SELECT COUNT(*) AS origin_inconsistencies
FROM transactions
WHERE id BETWEEN 1 AND 100000
  AND newbalanceOrig <> (oldbalanceOrg - amount);

-- Destination inconsistencies
SELECT COUNT(*) AS dest_inconsistencies
FROM transactions
WHERE id BETWEEN 1 AND 100000
  AND newbalanceDest <> (oldbalanceDest + amount);
  

CREATE TABLE balance_inconsistency_summary (
    chunk_id INT AUTO_INCREMENT PRIMARY KEY,
    origin_inconsistencies INT,
    dest_inconsistencies INT
);
  
DROP PROCEDURE IF EXISTS check_balance_consistency;
DELIMITER $$

CREATE PROCEDURE check_balance_consistency(
    IN chunk_size INT
)
BEGIN
    DECLARE start_id INT DEFAULT 0;
    DECLARE end_id INT;
    DECLARE max_id INT;
    DECLARE chunk_num INT DEFAULT 1;

    -- Get highest transaction ID
    SELECT MAX(id) INTO max_id FROM transactions;

    WHILE start_id < max_id DO
        SET end_id = start_id + chunk_size;

        -- Avoid duplicate primary key entries
        INSERT IGNORE INTO balance_inconsistency_summary (chunk_id, origin_inconsistencies, dest_inconsistencies)
        SELECT 
            chunk_num AS chunk_id,
            SUM(CASE 
                    WHEN newbalanceOrig != oldbalanceOrg - amount 
                         AND newbalanceOrig != oldbalanceOrg 
                    THEN 1 ELSE 0 END) AS origin_inconsistencies,
            SUM(CASE 
                    WHEN newbalanceDest != oldbalanceDest + amount 
                         AND newbalanceDest != oldbalanceDest 
                    THEN 1 ELSE 0 END) AS dest_inconsistencies
        FROM transactions
        WHERE id > start_id AND id <= end_id;

        SET start_id = end_id;
        SET chunk_num = chunk_num + 1;
    END WHILE;
END$$

DELIMITER ;

CALL check_balance_consistency(10000);

-- each chunk_id represents one block of 10,000 rows analyzed
SELECT * FROM balance_inconsistency_summary;

SELECT 
    SUM(origin_inconsistencies) AS total_origin_issues,
    SUM(dest_inconsistencies) AS total_dest_issues
FROM balance_inconsistency_summary;

SELECT COUNT(*) AS total_chunks FROM balance_inconsistency_summary;

CREATE OR REPLACE VIEW fraud_balance_summary AS
SELECT 
    SUM(origin_inconsistencies) AS total_origin_inconsistencies,
    SUM(dest_inconsistencies) AS total_dest_inconsistencies,
    (SUM(origin_inconsistencies) + SUM(dest_inconsistencies)) AS total_inconsistencies
FROM balance_inconsistency_summary;

SELECT * FROM fraud_balance_summary;

-- Finished with Balance Consistency checks between origin accounts and destination accounts


-- Now its time for the negative or zero amount transactions
CREATE TABLE IF NOT EXISTS transaction_amount_summary (
    check_id INT AUTO_INCREMENT PRIMARY KEY,
    total_transactions BIGINT,
    zero_amounts BIGINT,
    negative_amounts BIGINT,
    zero_fraud_cases BIGINT,
    negative_fraud_cases BIGINT,
    summary_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO transaction_amount_summary (total_transactions, zero_amounts, negative_amounts, zero_fraud_cases, negative_fraud_cases)
SELECT 
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_amounts,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    SUM(CASE WHEN amount = 0 AND isFraud = 1 THEN 1 ELSE 0 END) AS zero_fraud_cases,
    SUM(CASE WHEN amount < 0 AND isFraud = 1 THEN 1 ELSE 0 END) AS negative_fraud_cases
FROM transactions;

SELECT * FROM transaction_amount_summary;

-- Now we create a view for the tableau dashboard later, showing transaction integrity summaries
CREATE OR REPLACE VIEW fraud_zero_negative_summary AS
SELECT 
    total_transactions,
    zero_amounts,
    negative_amounts,
    zero_fraud_cases,
    negative_fraud_cases
FROM transaction_amount_summary
ORDER BY summary_date DESC
LIMIT 1;

-- Now we are making a Fraud v Flagged Fraud consistency check, the most important insight 

CREATE TABLE IF NOT EXISTS fraud_flag_summary (
    check_id INT AUTO_INCREMENT PRIMARY KEY,
    total_transactions BIGINT,
    total_frauds BIGINT,
    total_flagged BIGINT,
    correctly_flagged BIGINT,
    missed_fraud BIGINT,
    false_flags BIGINT,
    summary_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO fraud_flag_summary (
    total_transactions,
    total_frauds,
    total_flagged,
    correctly_flagged,
    missed_fraud,
    false_flags
)
SELECT 
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS total_frauds,
    SUM(CASE WHEN isFlaggedFraud = 1 THEN 1 ELSE 0 END) AS total_flagged,
    SUM(CASE WHEN isFraud = 1 AND isFlaggedFraud = 1 THEN 1 ELSE 0 END) AS correctly_flagged,
    SUM(CASE WHEN isFraud = 1 AND isFlaggedFraud = 0 THEN 1 ELSE 0 END) AS missed_fraud,
    SUM(CASE WHEN isFraud = 0 AND isFlaggedFraud = 1 THEN 1 ELSE 0 END) AS false_flags
FROM transactions;

SELECT * FROM fraud_flag_summary;

-- Now lets make a view for tableau, this gives me dectection accuracy rates I can visualize later
CREATE OR REPLACE VIEW fraud_detection_summary AS
SELECT 
    total_transactions,
    total_frauds,
    total_flagged,
    correctly_flagged,
    missed_fraud,
    false_flags,
    ROUND((correctly_flagged / total_frauds) * 100, 2) AS detection_rate,
    ROUND((false_flags / total_flagged) * 100, 2) AS false_positive_rate
FROM fraud_flag_summary
ORDER BY summary_date DESC
LIMIT 1;

SELECT * FROM balance_inconsistency_summary;


