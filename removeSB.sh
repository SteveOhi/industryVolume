#!/bin/bash

sed -i -e 's/\[//' industries_scraped.txt
sed -i -e 's/Price (Intraday)/          Volume/' industries_scraped.txt
