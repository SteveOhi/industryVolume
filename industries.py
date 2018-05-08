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
import lxml
import html5lib
import fileinput
import subprocess
import os
import logging
import time
import datetime
import psycopg2
import re
import csv
import numpy as np
import io

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
#  print(ind_data[0])
  subprocess.call("./insert_date.sh", shell=True)

# Determine if there has been any trading activity by comparing the current day's total volume
# against the previous day's total volume. If the two are different, it assumes that there was
# market activity and runs the rest of the program.
  ind_price = pd.read_csv("./tmp/ind_price")
#  current_index = ind_price.set_index("Symbol")

  previous = pd.read_csv("./tmp/ind_price_prev")
#  previous_index = previous.set_index("Symbol")
  if ind_price.iloc[0]['Price'] == previous.iloc[0]['Price']:
    print("No activity detected for", print (now))
    logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile.txt',level=logging.DEBUG)
    logging.warning("No trading activity detected today")


# At this point, you will need to write the date to an html page
    print ("{:,d}".format(current_index.loc["^TV.US"]["Price(Intraday)"]),"{:,d}".format(previous_index.loc["^TV.US"]["Price(Intraday)"]))
    return "no"

  else:
    print("Activity detected for", datetime.date.today().strftime("%B"), datetime.date.today().strftime("%d"), datetime.date.today().strftime("%Y"))
    print ("Difference ", ind_price.iloc[0]['Price'], " ", previous.iloc[0]['Price'])
# Remove the Total and Wholesale records from the current day's activity
    ind_price = ind_price[ind_price.Symbol != '^YHOH860']
    ind_price = ind_price[ind_price.Symbol != '^TV.US']
    ind_price.to_csv('./tmp/ind_price2', header=None, index=False, sep=',')

# Copy the ind_price.csv table to the ind_price postgres table
    db_string = "postgres://postgres:password@localhost:5432/postgres"
    conn = psycopg2.connect(db_string)
    cur = conn.cursor()
    with open('./tmp/ind_price2', "r") as infile:
      next (infile) # Skip the row header
      cur.copy_from(infile, 'ind_price', sep=',')
      conn.commit()
      cur.close()
    return "yes"
    infile.close()

# This part of the program creates and formats urls for each industry
#def format_industry_urls():
#  from random import randint
#  from time import sleep
#  sleep(randint(10,20))
#  sauce = urllib.request.urlopen('https://finance.yahoo.com/industries').read()
#  soup = bs.BeautifulSoup(sauce,'lxml')
#  f = open('./tmp/ind_urls', 'w')
#  for url in soup.find_all('a'):
#    print(url, end="", file=f, flush=False)
#  f.close()
# subprocess.call formats the hrefs to yahoo finance industry urls
#  subprocess.call("./href_edit.sh", shell=True)

# Read the yahoo finance industry urls into an array and for each array, run the volume comparisons on both the specific industry and with each stock
def get_stocks():
  with open('./tmp/url_data2', "r") as infile:
      content = infile.readlines()
# Skip reading the first line since it contains /industry/Wholesale_Other which is not an Industry
#      next(content, None)
      ind_df = pd.read_csv('./tmp/url_data2')
      url = ind_df.set_index("URL", drop = False)
      print (ind_df.columns)

#      for s in range (0, len(url.index)):
#        print (ind_df['URL'])
#        print (ind_df.iloc[0]['url'])
#ind_price.iloc[0]['Price']
      for s in range (0, len(ind_df.index)):
        url_link = ind_df.get_value(s, 'URL')
        ind_symbol = ind_df.iloc[s]['Symbol']
        print ('url_link ', url_link, 'Symbol ', ind_symbol)
        from random import randint
        from time import sleep
        sleep(randint(10,20))

        try:
           urllib.request.urlopen(url_link)
        except urllib.error.HTTPError as err:
           logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile',level=logging.DEBUG)
           logging.warning("Invalid URL for '{0}'".format(url_link))
        else:
           stock_data = open('./tmp/stock_data', 'w')
        try:
#           ind_df = pd.read_html(p,header=0)
           stock_data = pd.read_html(url_link, header=0)
#           ind_df = pd.read_csv('./tmp/url_data')
        except Exception as e:
           logging.basicConfig(format='%(asctime)s %(message)s',filename='logfile',level=logging.DEBUG)
           logging.warning("There are no stocks in '{0}'".format(url_link))
        else:
#           print('stock_data', stock_data[0].columns)
#           stock_data[0].to_csv('./tmp/stock_data', header=None, index=False, sep=',')
#           print (ind_df.columns)

# Yahoo Finance displays volumes using commas and M's (to denote millions).  This section normalizes the data to numeric values
           ind_vol = 0
           avg_ind_vol = 0
           for i in range (0, len(stock_data[0].index)):
             print (stock_data[0].iloc[i]['Volume'])
             if "M" in str(stock_data[0].iloc[i]['Volume']):
#               print (stock_data[0].iloc[i]['Volume'])
               tempVol = int(float(str(stock_data[0].iloc[i]['Volume']).replace("M", "")) * 1000000)
               ind_vol = int(ind_vol) + int(tempVol);
#               stock_data[0].loc[i, 'Volume'] = tempVol
#               stock_data[0].loc[i, 'Volume'] = int(stock_data[0].loc[i, 'Volume'])
#               print ('Updated volume ', stock_data[0].iloc[i]['Volume'])


# Checkout https://stackoverflow.com/questions/13842088/set-value-for-particular-cell-in-pandas-dataframe-using-index
             else:
               ind_vol = (stock_data[0].iloc[i]['Volume'])
             if "M" in str(stock_data[0].iloc[i]['Avg Vol (3 month)']):
               print (stock_data[0].iloc[i]['Avg Vol (3 month)'])
               tempVol = int(float(str(stock_data[0].iloc[i]['Avg Vol (3 month)']).replace("M", "")) * 1000000)
               stock_data[0].loc[i, 'Avg Vol (3 month)'] = tempVol
               stock_data[0].loc[i, 'Avg Vol (3 month)'] = int(stock_data[0].loc[i, 'Avg Vol (3 month)'])
               print ('tempVol ', tempVol, 'Updated ', (stock_data[0].iloc[i]['Avg Vol (3 month)']))
               avg_ind_vol = int(avg_ind_vol) + int(tempVol);
             else:
               avg_ind_vol = (stock_data[0].iloc[i]['Avg Vol (3 month)'])
# At this point you have the values to write directly to the database, I think
           print ('ind_vol ', ind_vol, 'avg_ind_vol ', avg_ind_vol)
           percent_change_volume = int(ind_vol)/int(avg_ind_vol)*100

           db_string = "postgres://postgres:password@localhost:5432/postgres"
           conn = psycopg2.connect(db_string)
           cur = conn.cursor()
           cur.execute("INSERT INTO ind_vol(ind_symbol, trade_date, ind_vol, avg_ind_vol, percent_change_volume) VALUES (%s, %s, %s, %s, %s)", (ind_symbol, now, int(ind_vol), int(avg_ind_vol), percent_change_volume))
           conn.commit()
           cur.close



#           print ('percent_change_volume ', percent_change_volume)
#           print ('ind_symbol ', ind_symbol)
#           print ('trade_date ', now)
#           print ('ind_vol ', ind_vol)
#           print ('avg_ind_vol ', avg_ind_vol)
#           print ('percent_change_volume', percent_change_volume)

#           print ('percent_change_volume ', type(percent_change_volume))
#           print ('ind_symbol ', type(ind_symbol))
#           print ('trade_date ', type(now))
#           print ('ind_vol ', type(ind_vol))
#           print ('avg_ind_vol ', type(avg_ind_vol))
#           print ('percent_change_volume', type(percent_change_volume))
#           print ('ind_symbol ', ind_symbol, 'trade_date ', now, 'ind_vol ', ind_vol, 'avg_ind_vol ', avg_ind_vol, 'percent_change_volume', percent_change_volume);

#             totalAvgVol = stock_data[0]['Avg Vol (3 month)'].sum()
#             stock_data[0].to_csv('./tmp/stock_data', header=None, index=False, sep=',')
#             if (totalVol) >= (totalAvgVol*2):
#               increase_in_volume = float(totalVol)/float(totalAvgVol)
#               final = open('increase_in_volume.txt', 'a')
#               print("The volume for ", industry, " was ",increase_in_volume, " times higher than the 30-day average.")
#             stock_data.close()
#             else:
#               print (industry, "Total volume is within the normal range",  "Total Volume ={:,d}".format(totalVol), "  Total Average Volume = {:,d}".format(totalAvgVol))
  infile.close()
#  stock_data.close()

if __name__ == '__main__':
   now = datetime.date.today()
#   scrape_industries()
#   if daily_activity() == "no":
#       print("daily_activity = no")
#   else:
#       print("daily_activity = yes")
#       format_industry_urls()
   get_stocks()

