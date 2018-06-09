# - *- coding: utf- 8 - *-
__author__ = 'abhinandan'
import sys
import subprocess
import threading
import csv
flag=0
def scheduler():
  global flag
  print("Scheduler Started")
  with open('source.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)
    for row in readCSV:
        jarpath = str(sys.argv[1])
        retailer=row[1].replace(" ", "").replace("'", "").replace(".", "").replace("’", "")
        retailer_lower=retailer.lower()
        esflag=row[12].strip()
        tgt_table_name=row[9].strip()

        #threading.Timer(300, scheduler).start()
        if(esflag.strip() and esflag=="1"):

          print("ES Load Started for Retailer "+retailer_lower)
          #print("Producer Started")
          subprocess.call(['java', '-jar', '-Xmx1024m', jarpath, 'ebay-multi', tgt_table_name, tgt_table_name, retailer_lower])


# start calling f now and every 2 mins thereafter
scheduler()
