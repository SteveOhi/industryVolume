#!/usr/bin/python3.5

# This program is used to match urls with Industry names from the finance.yahoo.com/Industries page as follows:
# The program first obtains the symbol and url using beautiful soup
# It then obtains the symbol and Industry name using dataframes
# Then using the symbol as the common field from each table, it then matches the Industry name with the url
# Finally, it stores the industry name and url in a table which can be referenced at a later date to see if there has been
# any changes in the urls or industry names

import bs4 as bs
import urllib.request
import pandas as pd
import subprocess
import os
import shutil
import logging
import datetime
import psycopg2
import csv
import numpy as np
import math
import locale
from sqlalchemy.exc import IntegrityError

# This program checks to see if there was a spike in volume in one of the industries in the NYSE.
# December 1, 2017 Steve Ohi

# logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile.txt',level=logging.DEBUG)
# logging.debug('Debug Level 1')
# logging.info('Info Level 1')
# logging.warning('Warning Level 1')

# scrape_industries() scrapes the list of industries from https://finance.yahoo.com/industries/
def scrape_industries():
# Start by scraping the industries table from Yahoo Finance into a pandas dataframe table
  ind_data = pd.read_html('https://finance.yahoo.com/industries/', header=0)
  pd.set_option('display.max_rows', 500)
# Save the dataframe to a csv (see http://www.pythonforbeginners.com/systems-programming/using-the-csv-module-in-python/)
  ind_data[0].to_csv('./tmp/ind_data', header=None, index=False, sep=',')
  subprocess.call("./insert_date.sh", shell=True)

# Determine if there has been any trading activity by comparing the current day's total volume against the previous day's total volume. If the two are different, it assumes that there was market activity and runs the rest of the program.
  ind_price = pd.read_csv("./tmp/ind_price")
  previous = pd.read_csv("./tmp/ind_price_prev")
  if ind_price.iloc[0]['Price'] == previous.iloc[0]['Price']:
    print("No activity detected for", print (now))
    logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile.txt',level=logging.DEBUG)
    logging.warning("No trading activity detected today")

# At this point, you will need to write the date to an html page
#    print ("{:,d}".format(current_index.loc["^TV.US"]["Price(Intraday)"]),"{:,d}".format(previous_index.loc["^TV.US"]["Price(Intraday)"]))
    return "False"
  else:
    print("Activity detected for", datetime.date.today().strftime("%B"), datetime.date.today().strftime("%d"), datetime.date.today().strftime("%Y"))
    print ("Difference ", ind_price.iloc[0]['Price'], " ", previous.iloc[0]['Price'])
# Remove the Total and Wholesale records from the current day's activity
    ind_price = ind_price[ind_price.Symbol != '^YHOH860']
    ind_price = ind_price[ind_price.Symbol != '^TV.US']
# Copy the ind_price.csv table to the ind_price postgres table
    ind_price.to_csv('./tmp/ind_price2', header=None, index=False, sep=',')

# Write the industry prices to the ind_price table
# To delete records, use delete from ind_price where trade_date = 'yyyy-mm-dd';
    conn = psycopg2.connect(dbname="industry_volumes", user="postgres", password="password")
#    db_string = "postgres://postgres:password@localhost:5432/postgres"
#    conn = psycopg2.connect(db_string)
    cur = conn.cursor()
    with open('./tmp/ind_price2', "r") as infile:
      next (infile) # Skip the row header
      cur.copy_from(infile, 'ind_price', sep=',')
      conn.commit()
      cur.close()
    infile.close()
#   Copy the contents of "./tmp/ind_price" to "./tmp/ind_price_prev" (uses shutil) to update the "if ind_price.iloc[0]['Price'] == previous.iloc[0]['Price']:" comparison
    shutil.copyfile('./tmp/ind_price', './tmp/ind_price_prev')
    return "True"

# This part of the program creates and formats urls for each industry
def format_industry_urls():
  html = urllib.request.urlopen('https://finance.yahoo.com/industries').read()
  soup = bs.BeautifulSoup(html, "html5lib")
  f = open('./tmp/url_data', 'w')
  table = soup.find('table')
  table_urls = table.find_all('a', href=True)
  print("Symbol," + "URL," + "Name", file=f, flush=False)
  for url in table_urls:
     href = url.get('href')
# Skip the total volume line
     if ("%5ETV" not in href):
# Skip the ISP industry
       if ("%5EYHOH860" not in href):
# Skip the Property and Casualty industry
           if ("%5EYHOH770" not in href):
# Skip the Wholesale industry
               if ("Wholesale_Other" not in href):
                   if ("p=" in href):
                       lhs, rhs = href.split("p=%5E", 1)
                       symbol = rhs.lstrip()
# The symbol is collected in the event that it is needed at a later date
                       symbol = "^" + symbol.rstrip()
                       print(symbol, end=",", file=f, flush=False)
                   if ("industry" in href):
                       lhs, industry = href.split("industry/", 1)
                       href = 'https://finance.yahoo.com' + href
                       print(href + "," + industry, end="\n", file=f, flush=False)
  f.close()

# Read the yahoo finance industry urls into an array and for each array, run the volume comparisons on both the specific industry and with each stock
def get_stocks():
  with open('./tmp/url_data', "r") as infile:
      content = infile.readlines()
# Skip reading the first line since it contains /industry/Wholesale_Other which is not an Industry
      ind_df = pd.read_csv('./tmp/url_data')
      url = ind_df.set_index("URL", drop = False)
# The ind_vol and avg_ind_vol variables need to be initialized since they will be used as quotients
      for s in range (0, len(ind_df.index)):
        if s == 0:
          ind_vol = 0
          avg_ind_vol = 0
        url_link = ind_df.get_value(s, 'URL')
        ind_symbol = ind_df.iloc[s]['Symbol']
        print ('url_link ', url_link, 'Symbol ', ind_symbol)
        from random import randint
        from time import sleep
        sleep(randint(10,20))

        try:
           stock_data = pd.read_html(url_link, header=0)
           with pd.option_context('display.max_rows', None, 'display.max_columns', None):
             print(stock_data)

        except Exception as e:
           logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile',level=logging.DEBUG)
           logging.warning("There are no stocks in '{0}'".format(url_link))
        else:

# Yahoo Finance displays volumes using commas and M's (to denote millions).  This section normalizes the data to numeric values
           for i in range (0, len(stock_data[0].index)):
             print ("stock_data[0].iloc[i]['Volume'] ", stock_data[0].iloc[i]['Volume'])
             if "M" in str(stock_data[0].iloc[i]['Volume']):
               tempVol = int(float(str(stock_data[0].iloc[i]['Volume']).replace("M", "")) * 1000000)
               ind_vol = int(ind_vol) + int(tempVol);
             elif math.isnan(float(stock_data[0].iloc[i]['Volume'])):
               stock_data[0].loc[i, 'Volume'] = int(ind_vol)
# Checkout https://stackoverflow.com/questions/13842088/set-value-for-particular-cell-in-pandas-dataframe-using-index
             else:
               ind_vol = ind_vol + int((stock_data[0].iloc[i]['Volume']))
             if "M" in str(stock_data[0].iloc[i]['Avg Vol (3 month)']):
               print (stock_data[0].iloc[i]['Avg Vol (3 month)'])
               tempVol = int(float(str(stock_data[0].iloc[i]['Avg Vol (3 month)']).replace("M", "")) * 1000000)
               stock_data[0].loc[i, 'Avg Vol (3 month)'] = tempVol
               stock_data[0].loc[i, 'Avg Vol (3 month)'] = int(stock_data[0].loc[i, 'Avg Vol (3 month)'])
               avg_ind_vol = int(avg_ind_vol) + int(tempVol);
             elif math.isnan(float(stock_data[0].iloc[i]['Avg Vol (3 month)'])):
               stock_data[0].loc[i, 'Avg Vol (3 month)'] = int(ind_vol)
             else:
               avg_ind_vol = avg_ind_vol+ int((stock_data[0].iloc[i]['Avg Vol (3 month)']))

# Write stock data to the database
             conn = psycopg2.connect(dbname="industry_volumes", user="postgres", password="password")
             cur = conn.cursor()
             try:
                 cur.execute("INSERT INTO stock_data (ind_symbol, stock_symbol, trade_date, stock_name, closing_price, price_change, price_percent_change, stock_volume, stock_avg_vol, mkt_cap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(ind_symbol), str(stock_data[0].iloc[i]['Symbol']), now, str(stock_data[0].iloc[i]['Name']), float(stock_data[0].iloc[i]['Price (Intraday)']), float(stock_data[0].iloc[i]['Change']), str(stock_data[0].iloc[i]['% Change']), str(stock_data[0].iloc[i]['Volume']), str(stock_data[0].iloc[i]['Avg Vol (3 month)']), str(stock_data[0].iloc[i]['Market Cap'])))
             except IntegrityError as e:
                 print ("IntegrityError")
                 logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile',level=logging.DEBUG)
                 logging.warning("Integrity Error for symbol '{0}'".format(str(stock_data[0].iloc[i]['Symbol'])))
                 conn.rollback()
             except Exception as e:
                 print ("Error")
                 conn.rollback()
             else:
                 conn.commit()
             cur.close
#             try:
#                cur = conn.cursor()
#                try:
#                    cur.execute("INSERT INTO stock_data (ind_symbol, stock_symbol, trade_date, stock_name, closing_price, price_change, price_percent_change, stock_volume, stock_avg_vol, mkt_cap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(ind_symbol), str(stock_data[0].iloc[i]['Symbol']), now, str(stock_data[0].iloc[i]['Name']), float(stock_data[0].iloc[i]['Price (Intraday)']), float(stock_data[0].iloc[i]['Change']), str(stock_data[0].iloc[i]['% Change']), str(stock_data[0].iloc[i]['Volume']), str(stock_data[0].iloc[i]['Avg Vol (3 month)']), str(stock_data[0].iloc[i]['Market Cap'])))
#                    except pyscopg2.IntegrityError:
#                        conn.rollback()
#                    else:
#                        conn.commit()
#                    cur.close
#                except Exception, e:
#                    print 'ERROR: ', e[0]

# Write industry volumes to the database

           percent_change_volume = int(ind_vol)/int(avg_ind_vol)*100
           conn = psycopg2.connect(dbname="industry_volumes", user="postgres", password="password")
           cur = conn.cursor()
           cur.execute("INSERT INTO ind_vol(ind_symbol, trade_date, ind_vol, avg_ind_vol, percent_change_volume) VALUES (%s, %s, %s, %s, %s)", (ind_symbol, now, int(ind_vol), int(avg_ind_vol), percent_change_volume))
           conn.commit()
           cur.close

  infile.close()


if __name__ == '__main__':
   now = datetime.date.today()
   if scrape_industries():
     print("daily_activity = yes")
     format_industry_urls()
     get_stocks()
   else:
     print("daily_activity = no")
