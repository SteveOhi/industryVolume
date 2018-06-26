host=localhost port=5432 dbname=industry_volumes connect_timeout=10

-- Add Users
CREATE USER app_ro WITH PASSWORD 'password';
CREATE USER app_rw WITH PASSWORD 'password';

-- Create DB
-- CREATE DATABASE industry_volumes;

-- login to the new DB
\c industry_volumes

-- Revoke all Privileges
REVOKE ALL ON DATABASE industry_volumes FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Set up privileges for app_ro
GRANT CONNECT ON DATABASE industry_volumes to app_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_ro;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO app_ro;
GRANT USAGE ON SCHEMA public to app_ro;


-- Set up privileges for app_rw
GRANT CONNECT ON DATABASE industry_volumes to app_rw;
GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES IN SCHEMA public TO app_rw;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO app_rw;
GRANT USAGE ON SCHEMA public to app_rw;


GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO app_ro;
GRANT USAGE ON SCHEMA public to app_ro;

-- Set up privileges for app_ro (for new tables)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON TABLES TO app_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON SEQUENCES TO app_ro;

-- Set up privileges for app_rw (for new tables)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT, UPDATE, INSERT, DELETE ON TABLES TO app_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT, UPDATE ON SEQUENCES TO app_rw;

--Create tables
CREATE TABLE ind_data (trade_date date, symbol text, ind_name text, price DECIMAL, PRIMARY KEY(symbol, trade_date));
CREATE TABLE ind_price (symbol text, date DATE NOT NULL DEFAULT CURRENT_DATE, price DECIMAL, primary key (symbol, date));
CREATE TABLE ind_vol (ind_symbol TEXT, trade_date DATE NOT NULL DEFAULT CURRENT_DATE, ind_vol BIGINT NOT NULL, avg_ind_vol BIGINT NOT NULL, percent_change_volume DECIMAL, PRIMARY KEY(ind_symbol, trade_date));
CREATE TABLE stock_data (ind_symbol TEXT, stock_symbol TEXT NOT NULL, trade_date DATE NOT NULL, stock_name TEXT NOT NULL, closing_price DECIMAL, price_change DECIMAL, price_percent_change TEXT, stock_volume TEXT, stock_avg_vol TEXT, mkt_cap TEXT, PRIMARY KEY(stock_symbol, trade_date));

