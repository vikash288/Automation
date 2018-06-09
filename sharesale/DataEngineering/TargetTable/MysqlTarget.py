import MySQLdb
import csv
import sys
# -*- coding: utf-8 -*-
__author__ = 'abhinandan'


staging_table_name="production.STG_THEMENSWEARHOUSE_LINKSHARE_W"
target_table_name="PRICE_DOT_COM_TRGT.TGT_THEMENSWEARHOUSE_LINKSHARE_W"
retailer_no="118"
ec2foldername=""
sqlfile_name="linksharesql.csv"
sqlfile_path="/home/abhinandan/Projects/Price.com/sql"
affilate_name=""
logfile_name=""
logpath=""


# Open database connection
db = MySQLdb.connect("crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com","crawler","price123","production" )

# prepare a cursor object using cursor() method
step1cursor = db.cursor()
step2cursor = db.cursor()
step3cursor = db.cursor()
step4cursor = db.cursor()
step5cursor = db.cursor()


with open(sqlfile_path+"/"+sqlfile_name) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)
    for row in readCSV:
      #Step 1 query
      print "Step 1 Query Started"
      step1=row[1]
      step1=step1.replace("$Y",retailer_no).replace("$X",staging_table_name)
      print "Step 1 Query: "+step1
      step1cursor.execute(step1)
      step1cursor.close()
      db.commit()


      #step2 Query
      print "Step 2 Query Started"
      step2=row[2]
      print("Step 2 Query: "+step2)
      step2cursor.execute(step2)
      db.commit()
      step2cursor.close()

      #Step 3 Query
      print("Step 3 Query Started")
      step3=row[3].replace("$Y",retailer_no).replace("$X",staging_table_name)
      print("Step 3 Query: "+step3)
      step3cursor.execute(step3)
      db.commit()
      step3cursor.close();

      #Step 4 Query
      print("Step 4 Query Started")
      step4=row[4]
      print("Step 4 Query: "+step4)
      step4cursor.execute(step4)
      db.commit()
      step4cursor.close()

      #Step 5 Query
      step5=row[5].replace("$Y",retailer_no).replace("$X",staging_table_name).replace("$Z",target_table_name)
      print("Step 5 Query: "+step5)
      step5cursor.execute(step5)
      db.commit()
      step5cursor.close()

      print("Target table created: "+target_table_name)


      db.close()
      sys.exit(0)
