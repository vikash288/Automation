#!/bin/bash

python walmart_feed.py $1 > /home/ubuntu/affiliate/walmart/data/$$.tsv

echo "LOAD DATA LOCAL INFILE '/home/ubuntu/affiliate/walmart/data/$$.tsv' REPLACE INTO TABLE affiliate.walmart_feed FIELDS TERMINATED BY '\t'" | mysql -u crawler -h crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com -P 3306 -pprice123

rm /home/ubuntu/affiliate/walmart/data/$$.tsv

