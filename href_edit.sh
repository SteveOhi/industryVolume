#!/bin/bash

# Remove text prior to the first industry
sed -i -e 's/^.*href="\/industry\/Aluminum"/href="\/industry\/Aluminum"/p' href_scraped.txt
# echo "Debug 1"

# Remove text after the last stock
sed -i -e 's/Home Furn Stores.*/Home Furn Stores/' href_scraped.txt
# echo "Debug 2"

# Start a new line where an href occurs
sed -i -e 's/href/\nhref/g' href_scraped.txt
# echo "Debug 3"

# Start a new line when the word "title appears
sed -i -e 's/title/\n/g' href_scraped.txt
# echo "Debug 4"

# Print only those lines starting with an <href="industry ... (https://stackoverflow.com/questions/13202715/sed-get-lines-beginning-with-rim-cod)
awk '/href=\"\/industry/' href_scraped.txt > industry_urls.txt
# echo "Debug 5"

# Replace the href=" with https://finance.yahoo.com
sed -i -e 's/href=/https:\/\/finance.yahoo.com/g' industry_urls.txt
# echo "Debug 6"

# Remove the " character
sed -i -e 's/\"//g' industry_urls.txt
# echo "Debug 7"
