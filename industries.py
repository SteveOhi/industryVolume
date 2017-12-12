import bs4 as bs
import urllib.request
import pandas as pd
import fileinput
import subprocess
import os
import logging
import time
import datetime

# This program checks to see if there was a spike in volume in one of the industries in the NYSE.  
# December 1, 2017 Steve Ohi

# logging.basicConfig(format='%(asctime)s %(message)s', filename='logfile.txt',level=logging.DEBUG)
# logging.debug('Debug Level 1')
# logging.info('Info Level 1')
# logging.warning('Warning Level 1')

# scrape_industries() scrapes the list of industries from https://finance.yahoo.com/industries/
def scrape_industries():
  ''' Start by pulling the industries table from Yahoo Finance into a pandas dataframe table '''
  dfs = pd.read_html('https://finance.yahoo.com/industries/',header=0)
  pd.set_option('display.max_rows', 500)
  f = open('industries_scraped.txt', 'w')
  print(dfs, end="", file=f, flush=False)
  f.close()
  # subprocess.call removes the first square bracket from the industries_scraped.txt and print the output to industries_current.txt
  subprocess.call("./removeSB.sh", shell=True)

# daily_activity() determines whether there has been any trading activity during the current day.  It compares the current day's total volume against the previous day's total volume. If the two are different, it assumes that there was market activity and runs the rest of the program.  If the two are the same, it assumes that the market's were not open today and exits the program
def daily_activity():
# Using pandas dataframes, this sections defines the columns widths from the from the scrape_industries function
  path = "industries_scraped.txt"
  col_specification =[(0, 5), (6, 13), (14, 33), (34, 51), (52, 60), (61, 69)]
  current = pd.read_fwf(path, width=col_specification)
  current_index = current.set_index("Symbol")
# Using pandas dataframes, this sections defines the columns widths from the from the scrape_industries function for the previous day
  path = "industries_previous.txt"
  col_specification =[(0, 5), (6, 13), (14, 33), (34, 51), (52, 60), (61, 69)]
  previous = pd.read_fwf(path, width=col_specification)
  previous_index = previous.set_index("Symbol")

  if float(current_index.loc["^TV.US"]["Volume"]) == float(previous_index.loc["^TV.US"]["Volume"]):
    industry = "test"
    logging.basicConfig(format='%(asctime)s %(message)s', filename='logfile.txt',level=logging.DEBUG)
    logging.warning("No trading activity detected today")
    print("No activity detected for ", today)
    print ("{:,d}".format(current_index.loc["^TV.US"]["Volume"]), "{:,d}".format(previous_index.loc["^TV.US"]["Volume"]))
    return "no"
  else:
    print("Activity detected for ", datetime.date.today().strftime("%B"), datetime.date.today().strftime("%d"), datetime.date.today().strftime("%Y"))
    # This next line overwrites the previous volume with the latest volume copies the industries_current.txt file to the industries_previous.txt file
    with open('industries_previous.txt', 'w+') as output, open('industries_current.txt', 'r') as input:
      output.write(input.read())
    return "yes"

# This part of the program creates and formats urls for each industry
def format_industry_urls():
  from random import randint
  from time import sleep
  sleep(randint(10,20))
  sauce = urllib.request.urlopen('https://finance.yahoo.com/industries').read()
  soup = bs.BeautifulSoup(sauce,'lxml')
  f = open('href_scraped.txt', 'w')
  for url in soup.find_all('a'):
    print(url, end="", file=f, flush=False)
  f.close()
# subprocess.call formats the hrefs to yahoo finance industry urls
  subprocess.call("./href_edit.sh", shell=True)

# Read the yahoo finance industry urls into an array and for each array, run the volume comparisons on both the specific industry and with each stock
def get_stocks():
  with open('industry_urls.txt') as f:
      content = f.readlines()
      for p in content:
        from random import randint
        from time import sleep
        sleep(randint(10,20))
        lhs, industry = p.split("industry/", 1)
        txt = ".txt"
        industry = industry.rstrip() + txt
        try:
           urllib.request.urlopen(p)
        except urllib.error.HTTPError as err:
           logging.basicConfig(format='%(asctime)s %(message)s', filename='logfile.txt',level=logging.DEBUG)
           logging.warning("Invalid URL for '{0}'".format(industry))
        else:
           f = open(industry, 'w')
        try:
           ind_df = pd.read_html(p,header=0)
        except Exception as e: 
           logging.basicConfig(format='%(asctime)s %(message)s', filename='logfile.txt',level=logging.DEBUG)
           logging.warning("There are no stocks in '{0}'".format(industry))
        else:
           pd.set_option('display.max_rows', 500, 'display.width', 1000)
           print(ind_df, end="", file=f, flush=False)
           f.close()
	   # The following subprocess.call functions normalize the column headers
           subprocess.call(["sed", "-i", "-e",  's/\[//g', industry])
           subprocess.call(["sed", "-i", "-e",  's/Price (Intraday)/           Price/g', industry])
           subprocess.call(["sed", "-i", "-e",  's/Avg Vol (3 month)/           AvgVol/g', industry])
           subprocess.call(["sed", "-i", "-e",  's/% Change/ %Change/g', industry])
           subprocess.call(["sed", "-i", "-e",  's/Market Cap/ MarketCap/g', industry])
           subprocess.call(["sed", "-i", "-e",  's/PE Ratio (TTM)/       PERatio/g', industry])
           subprocess.call(["sed", "-i", "-e",  's/52 Week Range/  52WeekRange/g', industry])
           subprocess.call(["awk", "-v", "x=tmp.txt", '{printf "%6s %8s %10s %10s %10s %12s \\n", $2, $(NF-7), $(NF-4), $(NF-3), $(NF-2), $(NF) > x;}', industry ])
           from shutil import copyfile
           copyfile("tmp.txt", industry)        
           statinfo = os.stat(industry)

	   # The if statement checks to see if the list of stocks in an industry is zero to prevent an error when reading the file into a dataframe
           if statinfo.st_size == 0:
             print (industry, " file size is 0 bytes.")
           else:
             from random import randint
             from time import sleep
             sleep(randint(10,20))
             col_specification =[(0, 5), (6, 14), (15, 26), (27, 37), (38, 48), (49, 61)]
             data = pd.read_fwf(industry, width=col_specification)

           # Yahoo Finance displays volumes using commas and M's (to denote millions).  This section normalizes the data to numeric values
             for i in range (0, len(data.index)):
               if "M" in str(data.iloc[i, data.columns.get_loc('Volume')]):      
                 tempVol = str(data.iloc[i, data.columns.get_loc('Volume')]).replace("M", "")
                 tempVol = float(tempVol) * 1000000
                 tempVol = int(tempVol)
                 data.at[i, 'Volume'] = tempVol
               else:
                 tempVol = int(data.iloc[i, data.columns.get_loc('Volume')])
                 data.at[i, 'Volume'] = tempVol
               if (pd.isnull(data.iloc[i, data.columns.get_loc('AvgVol')])):
                 data.at[i, 'AvgVol'] = data.at[i, 'Volume']
               elif "M" in str(data.iloc[i, data.columns.get_loc('AvgVol')]):  
                 tempVol = str(data.iloc[i, data.columns.get_loc('AvgVol')]).replace("M", "")
                 tempVol = float(tempVol) * 1000000
                 tempVol = int(tempVol)
                 data.at[i, 'AvgVol'] = tempVol
               else:
                 tempVol = int(data.iloc[i, data.columns.get_loc('AvgVol')])
                 data.at[i, 'AvgVol'] = tempVol

             totalVol = data['Volume'].sum()
             totalAvgVol = data['AvgVol'].sum()
             if (totalVol) >= (totalAvgVol*2):
               increase_in_volume = float(totalVol)/float(totalAvgVol)
               final = open('increase_in_volume.txt', 'a')
               print("The volume for ", industry, " was ", increase_in_volume, " times higher than the 30-day average.")
               final.close()
             else:
               print (industry, "Total volume is within the normal range",  "Total Volume ={:,d}".format(totalVol), "  Total Average Volume = {:,d}".format(totalAvgVol))

if __name__ == '__main__':
   scrape_industries()
   if daily_activity() == "no":
       print("daily_activity = no")
   else:
       print("daily_activity = yes")
       format_industry_urls()
       get_stocks()
