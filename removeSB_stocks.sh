#!/bin/bash

sed 's/\[//' stocks_scraped.txt > stocks_temp.txt
sed 's/Avg Vol (3 month)/  Avg Vol 3 month/' stocks_temp.txt > stocks_current.txt

# cp industries_current.txt industries_previous.txt

# sleep 10
