#!/bin/bash

# This script formats the ind_data to print the Symbol, Date (system generated) and price

sed -i -e 's/ Other/1.0/' ./tmp/ind_data

awk -F "\"*,\"*" '{print $1 "," strftime("%Y-%m-%d") "," $3 }' ./tmp/ind_data > ./tmp/ind_data2

gawk 'BEGIN {print "Symbol,Date,Price"}
{if(NR > 0) print $1 "" $2 "" $3}' ./tmp/ind_data2 > ./tmp/ind_price



