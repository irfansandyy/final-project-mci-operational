-- 1. Create the dedicated Jupyter user
CREATE USER IF NOT EXISTS jupyter_reader IDENTIFIED WITH sha256_password BY 'clickhouse123';

-- 2. Strip all default privileges (Zero-Trust)
REVOKE ALL ON *.* FROM jupyter_reader;

-- 3. Grant SELECT strictly on the analytical tables
GRANT SELECT ON operational_db.dim_sellers TO jupyter_reader;
GRANT SELECT ON operational_db.dim_customers TO jupyter_reader;
GRANT SELECT ON operational_db.dim_reviews TO jupyter_reader;
GRANT SELECT ON operational_db.fact_deliveries TO jupyter_reader;
