__author__ = 'abhinandan'
import csv
import sys
import subprocess
# -*- coding: utf-8 -*-

#Read csv File to get LinkShare Config file
with open("linksharesource.csv") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV) #skip the header
    for row in readCSV:
        jarpath = str(sys.argv[1])
        loadtype = str(sys.argv[2])
        retailercode = row[0].strip()
        retailer=row[1].strip()
        affiliator=row[2].strip()
        connectionType = row[3].strip()
        ftpusername=row[4].strip()
        ftppassword=row[5].strip()
        ftphostname=row[6].strip()
        port="21"
        foldername=row[7].strip()
        sourcefile=row[8].strip()
        incrementalfile = row[9].strip()
        delimiter = row[10].strip()
        stg_table_name=row[11].strip()
        tgt_table_name=row[12].strip()
        fullload_flag = row[13].strip()
        incremental_flag = row[14].strip()
        s3flag=row[15].strip()
        mysqlflag=row[16].strip()
        esflag=row[17].strip()

        if(loadtype=="full" and fullload_flag=="1" and s3flag=="1" and mysqlflag=="1" and esflag=="1"):
            subprocess.call(['java', '-jar', '-Xmx512m', jarpath, 'deleteautomation', ftphostname, ftpusername, ftppassword, port, foldername, sourcefile, retailer, affiliator, delimiter,retailercode,loadtype,s3flag,mysqlflag,esflag])
        else:
            print("Execution Halted, Please Add proper parameter value on the CSV file mention the load type and process that you want to execute")
        
