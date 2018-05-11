#!/usr/bin/python3.5

# This program is used to match urls with Industry names from the finance.yahoo.com/Industries page as follows:
# The program first obtains the symbol and url using beautiful soup
# It then obtains the symbol and Industry name using dataframes
# Then using the symbol as the common field from each table, it then matches the Industry name with the url
# Finally, it stores the industry name and url in a table which can be referenced at a later date to see if there has been
# any changes in the urls or industry names

import psycopg2


db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("DROP TABLE stock_data;")
conn.commit()
cur.close

db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("CREATE TABLE stock_data (ind_symbol TEXT, stock_symbol TEXT NOT NULL, trade_date DATE NOT NULL, stock_name TEXT NOT NULL, closing_price TEXT, price_change TEXT, price_percent_change TEXT, stock_volume TEXT, stock_avg_vol TEXT, mkt_cap TEXT, PRIMARY KEY(stock_symbol, trade_date))");
conn.commit()
cur.close

db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("DROP TABLE ind_price;")
conn.commit()
cur.close

db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("CREATE TABLE ind_price (symbol text, date DATE NOT NULL DEFAULT CURRENT_DATE, price DECIMAL, primary key (symbol, date))");
conn.commit()
cur.close

db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("DROP TABLE ind_vol;")
conn.commit()
cur.close

db_string = "postgres://postgres:password@localhost:5432/postgres"
conn = psycopg2.connect(db_string)
cur = conn.cursor()
cur.execute("CREATE TABLE ind_vol (ind_symbol TEXT, trade_date DATE NOT NULL DEFAULT CURRENT_DATE, ind_vol BIGINT NOT NULL, avg_ind_vol BIGINT NOT NULL, percent_change_volume DECIMAL, PRIMARY KEY(ind_symbol, trade_date))");
conn.commit()
cur.close




