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
        topic = str(sys.argv[2])
        ftphostname=row[5].strip()
        ftpusername=row[3].strip()
        ftppassword=row[4].strip()
        port="21"
        retailer=row[1].replace(" ", "").replace("'", "").replace(".", "").replace("’", "")
        retailer_lower=retailer.lower()
        sourcename=row[2].strip()
        foldername=row[6].lstrip('/')
        sourcefile=row[7].strip()
        stg_table_name=row[8].strip()
        tgt_table_name=row[9].strip()
        s3flag=row[10].strip()
        mysqlflag=row[11].strip()
        esflag=row[12].strip()


        #threading.Timer(300, scheduler).start()
        if(s3flag.strip() and s3flag=="1" and sourcename!="Pepper Jam"):

          print("FTP to S3 Load Started for Retailer: "+retailer)
          subprocess.call(['java', '-jar', '-Xmx512m', jarpath, 's3load', ftphostname, ftpusername, ftppassword, port, retailer, foldername, sourcefile, topic, retailer])
         
        elif(s3flag=="1" and sourcename=="Pepper Jam"):
          print("Direct Load Started: "+retailer)
          subprocess.call(['java', '-jar', '-Xmx512m', jarpath, 'directload', sourcefile, topic, retailer])

      


# start calling f now and every 2 mins thereafter
scheduler()
