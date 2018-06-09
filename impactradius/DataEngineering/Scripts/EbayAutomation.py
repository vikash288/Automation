import os
import sys
import urllib
import boto
import datetime
import subprocess
import requests, time
import gzip
import warnings
import MySQLdb
import csv
import logging
from StringIO import StringIO
from boto.s3.key import Key
# -*- coding: utf-8 -*-
__author__ = 'abhinandan'

warnings.filterwarnings('ignore')

#log properties
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logpath = '/home/ubuntu/log/automationlog/ebay/ebay_'+str(datetime.datetime.today().strftime('%Y-%m-%d'))+'.log'
directory = os.path.dirname(logpath)
if not os.path.exists(directory):
    os.makedirs(directory)

# create a file handler
handler = logging.FileHandler(logpath)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


#--------------------------S3 File Upload Using Boto--------------------------------------------
def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.

    callback is a function of the form:

    def callback(complete, total)

    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.

    Returns boolean indicating success/failure of upload.
    """
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)
    logger.info('file uploaded in bucket: '+str(bucket)+' key: '+str(key))
    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False
#----------------------------------------------------------------------


#----------Fetch Data From API and Write as a CSV----------------------

def get_ebay_data(date,category,path):

 try:
    BASE_URL = "https://epn.ebay.com/downloadFeeds"

    data = {
    "date": str(date),
    "feedName": 1,
    "programs": 1,
    "feedType": "daily",
    "categories": category,
    "username": "affiliate@price.com",
    "password": "Price$2017$"


    }
    print data
    response = requests.post(BASE_URL, data=data)
    logger.info("Request to ebay API "+date+" response code "+str(response.status_code))
    # this is gz data
    buf_data = StringIO(response.content)
    # Lets decompress
    f = gzip.GzipFile(fileobj=buf_data)

    with open(path+"/"+category+".txt", "w") as text_file:
        logger.info('File write started for category: '+category+' file location: '+path+"/"+category+".txt")
        for row in f.readlines():
            line = ",".join([each.replace(","," ").replace(";"," ") for each in row.split("\t")])
            text_file.write(line)

 except Exception as e:
        logger.error("Found Exception to download file for category: "+str(category)+" Exception cause: "+str(e))
        pass

#----------------------------------------------------------------------



#---------------------Mysql Staging table -----------------------------

def load_mysql_staging(dbconn,staging_table_name,filepath):

    truncatetable_cursor = dbconn.cursor()
    loaddate_cursor = dbconn.cursor()
    #Truncate Table
    truncatetable_cursor.execute("truncate "+staging_table_name)
    truncatetable_cursor.close();
    #Load Data into table
    loaddate_cursor.execute("load data local infile '"+filepath+"' into table "+staging_table_name+" fields terminated by ',' SET FIELD_51 = NOW()")
    loaddate_cursor.close()
    dbconn.commit()
    print("Data Load into Staging table : "+staging_table_name)
    logger.info("Data Load into Staging table : "+staging_table_name)

#----------------------------------------------------------------------

#---------------------Create Folder if not exits-----------------------

def create_folder_if_not_exits(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


#------------------------Truncate target Table--------------------------
def truncate_target_table(db,target_table_name,target_previous_table_name):
    movedatacursor = db.cursor()
    truncatepreviouscursor = db.cursor()
    truncatecursor = db.cursor()

    print "Truncate Previous table Query Started"
    logger.info("Truncate Previous table Query Started")
    pretruncate="Truncate "+target_previous_table_name
    print("Truncate table Query: "+pretruncate)
    logger.info("Truncate table Query: "+pretruncate)
    truncatepreviouscursor.execute(pretruncate)

    print "Move Data Query Started"
    logger.info("Move Data Query Started")
    movedata = "insert into "+target_previous_table_name+" select ITEM_ID,TITLE,UPC,PRODUCT_URL,IMAGE_URL,AFFILIATE_URL,PRICE_ID,PRICE1,PRICE2,PRODUCT_CATEGORY,PRODUCT_CONDITION,TOP_RATED_SELLER,SELLER_RATING_PERCENTAGE, now() as TIMEAUDIT from "+target_table_name;
    print("Move data Query: "+movedata)
    logger.info("Move data Query: "+movedata)
    movedatacursor.execute(movedata)

    print "Truncate Query Started"
    logger.info("Truncate Query Started")
    truncate="Truncate "+target_table_name
    print("Truncate table Query: "+truncate)
    logger.info("Truncate table Query: "+truncate)
    truncatecursor.execute(truncate)
    db.commit()
    movedatacursor.close()
    truncatecursor.close()
    truncatepreviouscursor.close()

#--------------------------------------------------

#------------------Load data into non match UPC table--------------------
def load_data_into_nonupc_table(db,target_table_name,target_previous_table_name,not_upc_match_table_name):
    truncatecursor= db.cursor();
    loadnonupcursor = db.cursor()
    print "Truncate Non UPC table"
    logger.info("Truncate Non UPC table")
    truncate="Truncate "+not_upc_match_table_name
    print("Truncate table Query: "+truncate)
    logger.info("Truncate table Query: "+truncate)
    truncatecursor.execute(truncate)

    print "Get non match UPC"
    logger.info("Get non match UPC")
    nonmatchupc = "insert into "+not_upc_match_table_name+" SELECT SRC.ITEM_ID,SRC.TITLE,SRC.UPC,SRC.PRODUCT_URL,SRC.IMAGE_URL,SRC.AFFILIATE_URL,SRC.PRICE_ID,SRC.PRICE1,SRC.PRICE2,SRC.PRODUCT_CATEGORY,SRC.PRODUCT_CONDITION,SRC.TOP_RATED_SELLER,SRC.SELLER_RATING_PERCENTAGE,now() as TIMEAUDIT FROM "+target_previous_table_name+" SRC LEFT OUTER JOIN "+target_table_name+" TGT ON SRC.UPC = TGT.UPC WHERE TGT.UPC IS NULL"
    print("Non UPC data " +nonmatchupc)
    logger.info("Non UPC data " +nonmatchupc)
    loadnonupcursor.execute(nonmatchupc)
    db.commit()
    truncatecursor.close()
    loadnonupcursor.close()
#--------------------------------------------------


#-----------------------Delete record from non UPC from ES---------------------------

def delete_non_upc_records_es(db,ESurl,not_upc_match_table_name):
    logger.info("Delete Stale UPC started")
    try:
        upc_not_match_cursor =  db.cursor()
        nonupc_query = "SELECT * FROM "+not_upc_match_table_name;
        upc_not_match_cursor.execute(nonupc_query)
        results = upc_not_match_cursor.fetchall()
        for row in results:
            response = requests.delete(ESurl+str(row[2]))
            print(response.text)
    except:
        pass


#--------------------------------------------------




#---------------------Delete File From Folder--------------------------

def delete_ebay_local_files(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
        logger.info("Delete File from:  " +filepath)
    else:
        print("Sorry, I can not remove %s file." % filename)

#--------------------------------------------------



#---------------------Mysql Staging table -----------------------------
def create_target_table(db,retailer_no,staging_table_name,target_table_name):
  logger.info("target table creation started, retailer no: "+retailer_no+" staging table name: "+staging_table_name+" target table name: "+target_table_name)
  try:
        # prepare a cursor object using cursor() method
        step1cursor = db.cursor()
        step2cursor = db.cursor()
        step3cursor = db.cursor()
        step4cursor = db.cursor()
        step5cursor = db.cursor()
        step6cursor = db.cursor()
        step7cursor = db.cursor()


        #Step 1 query
        print "Step 1 Query Started"
        step1="Insert into UPC_COMMON_ID_MAP2 Select TGT.COMMON_ID, SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID, now() from (SELECT FIELD_20 AS UPC,$Y AS RETAILER_CODE,FIELD_12 AS ITEM_ID FROM $X WHERE FIELD_20 <> 'UPC' AND FIELD_20 IS NOT NULL AND FIELD_20 <> ' ' AND FIELD_20 REGEXP '^[0-9]+$' GROUP BY UPC, RETAILER_CODE) SRC JOIN UPC_COMMON_ID_MAP2 TGT ON SRC.UPC = TGT.UPC and TGT.RETAILER_CODE <> $Y and TGT.RETAILER_CODE is null group by 1,2,3,4"
        step1=step1.replace("$Y",retailer_no).replace("$X",staging_table_name)
        print "Step 1 Query: "+step1
        step1cursor.execute(step1)
        step1cursor.close()
        db.commit()


        #step2 Query
        print "Step 2 Query Started"
        step2="Truncate UPC_COMMON_ID_MAP_W3"
        print("Step 2 Query: "+step2)
        step2cursor.execute(step2)
        db.commit()
        step2cursor.close()

        #Step 3 Query
        print("Step 3 Query Started")
        step3="Insert into UPC_COMMON_ID_MAP_W3 (UPC,RETAILER_CODE,ITEM_ID,AUDITTIME) Select SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID, now() from (SELECT FIELD_20 AS UPC, $Y AS RETAILER_CODE, FIELD_12 AS ITEM_ID FROM $X WHERE FIELD_20 <> 'UPC' AND FIELD_20 IS NOT NULL AND FIELD_20 <> ' '  AND FIELD_20 REGEXP '^[0-9]+$' GROUP BY UPC, RETAILER_CODE) SRC LEFT JOIN UPC_COMMON_ID_MAP2 TGT ON SRC.UPC = TGT.UPC where TGT.UPC is null"
        step3=step3.replace("$Y",retailer_no).replace("$X",staging_table_name)
        print("Step 3 Query: "+step3)
        step3cursor.execute(step3)
        db.commit()
        step3cursor.close();

        #Step 4 Query
        print("Step 4 Query Started")
        step4="Insert into UPC_COMMON_ID_MAP2 Select common_id + (Select max(common_id) from UPC_COMMON_ID_MAP2), upc, retailer_code, ITEM_ID, now() from UPC_COMMON_ID_MAP_W3;"
        print("Step 4 Query: "+step4)
        step4cursor.execute(step4)
        db.commit()
        step4cursor.close()

        #step5 Query
       # print "Step 5 Query Started"
       # step5="Truncate "+target_table_name
       # print("Step 5 Query: "+step5)
       # step5cursor.execute(step5)
       # db.commit()
       # step5cursor.close()

        #Step 6 Query
        #step6="create table $Z select a.FIELD_12 as ITEM_ID,a.FIELD_17 as TITLE,a.FIELD_20 as UPC,a.FIELD_7 as PRODUCT_URL,a.FIELD_6 as IMAGE_URL,a.FIELD_7 as AFFILIATE_URL,b.common_id as PRICE_ID,a.FIELD_11 + ' ' AS PRICE1,a.FIELD_5 + ' ' AS PRICE2,a.FIELD_9 as PRODUCT_CATEGORY,a.FIELD_19 as PRODUCT_CONDITION,a.FIELD_23 as TOP_RATED_SELLER,a.FIELD_24 as SELLER_RATING_PERCENTAGE from $X a, UPC_COMMON_ID_MAP2 b where a.FIELD_20 = b.upc and a.FIELD_19='Used' and b.retailer_code = $Y"
        step6="insert into $Z select a.FIELD_12 as ITEM_ID,a.FIELD_17 as TITLE,a.FIELD_20 as UPC,a.FIELD_7 as PRODUCT_URL,a.FIELD_6 as IMAGE_URL,a.FIELD_7 as AFFILIATE_URL,b.common_id as PRICE_ID,a.FIELD_11 + ' ' AS PRICE1,a.FIELD_5 + ' ' AS PRICE2,a.FIELD_9 as PRODUCT_CATEGORY,a.FIELD_19 as PRODUCT_CONDITION,a.FIELD_23 as TOP_RATED_SELLER,a.FIELD_24 as SELLER_RATING_PERCENTAGE , now() as TIMEAUDIT from $X a, UPC_COMMON_ID_MAP2 b where a.FIELD_20 = b.upc and a.FIELD_19='Used' and b.retailer_code = $Y"
        step6=step6.replace("$Y",retailer_no).replace("$X",staging_table_name).replace("$Z",target_table_name)
        print("Step 6 Query: "+step6)
        step6cursor.execute(step6)
        db.commit()
        step6cursor.close()

        #Step 7 Query
        print("Step 7 Started")
        step7="select count(*) from "+target_table_name
        step7cursor.execute(step7)
        print("Step 7 Query "+step7)
        result = step7cursor.fetchone()
        step7cursor.close();

        print("\nData Loaded into Table : "+target_table_name + " Total Row Count "+str(result[0]))
        logger.info("\nData Loaded into Table : "+target_table_name + " Total Row Count "+str(result[0]))

  except:
      pass

#--------------------------------------------------



if __name__ == "__main__":

    #AWS S3 Access Key
    AWS_ACCESS_KEY = 'AKIAJZQU3GLH7LWFXCVA'
    #AWS S3 Secret Key
    AWS_ACCESS_SECRET_KEY = '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'
    #Bucket Name
    AWS_BUCKET_NAME = 'price-data-engineering'
    #Retailer Number
    EBAY_USED_RETAILER_NO='120'
    #MySQL target Table Name for ebay used
    target_table_name = "PRICE_DOT_COM_TARGET.EBAY_USED_ITEMS"
    #Target table data for provious day
    target_previous_table_name = "PRICE_DOT_COM_TARGET.EBAY_USED_ITEMS_PREVIOUS"
    #Not match UPC
    not_upc_match_table_name = "PRICE_DOT_COM_TARGET.EBAY_USED_ITEMS_STALE_UPC"

    #Mysql RDS Database Connection
    db = MySQLdb.connect("crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com","crawler","price123","PRICE_DOT_COM_STAGING" , local_infile = 1 )

    #File path location
    filepath = '/home/ubuntu/DUMP'

    #Jar path Location
    jarpath="/home/ubuntu/ebaypush/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"

    #ES Link
    ESurl= "http://search-price-used-production-wrobho4tskcnz4mxygweriempe.us-east-1.es.amazonaws.com/product_index/product/"

    #Create filepath if not exits
    create_folder_if_not_exits(filepath)

    #Truncate Table before loading
    truncate_target_table(db,target_table_name,target_previous_table_name)


#Read csv File to get Ebay Staging table by category
with open("ebaytables.csv") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV) #skip the header
    for row in readCSV:
        category = row[2]
        staging_table_name = row[3]

        s3flag = row[5]
        mysqlflag = row[6]
        espushflag = row[7]


        filelocation = filepath+"/"+category+'.txt';
        todaydate = (datetime.datetime.today() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d')
        #todaydate = datetime.datetime.today().strftime('%Y-%m-%d')



        if s3flag == '1':

            #get data from Ebay API
            get_ebay_data(todaydate,category,filepath)

            file = open(filelocation, 'r+')
            filename = os.path.basename(filelocation)
            key = "/"+todaydate+"/ebay/"+filename

            #load data into S3
            if upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, file, AWS_BUCKET_NAME, key):
                print 'File Uploaded into S3 Bucket Key: '+AWS_BUCKET_NAME+key;
                logger.info('File Uploaded into S3 Bucket Key: '+AWS_BUCKET_NAME+key)
            else:
                logger.error('The upload failed for category: '+category)
                print 'The upload failed...'
                #sys.exit(0)


        if mysqlflag == '1':
            #File Load into MySql Staging table
            load_mysql_staging(db,staging_table_name,filelocation)

            # Delete File From Local
            delete_ebay_local_files(filelocation)

            #move data from target to preivious day record

            #Create Taget Table from Staging table
            create_target_table(db,EBAY_USED_RETAILER_NO,staging_table_name,target_table_name)


    #load data into Stale UPC table
    load_data_into_nonupc_table(db,target_table_name,target_previous_table_name,not_upc_match_table_name)

    #Remove non match UPC from ES
    delete_non_upc_records_es(db,ESurl,not_upc_match_table_name)

    print("Script Execution Complete")
    logger.info("Script Execution Complete")
    db.close()
    print("ES Push Started")
    logger.info("ES Push Started")
    subprocess.call(['java', '-jar', '-Xmx2024m', jarpath, 'ebay-multi', target_table_name, target_table_name, 'ebay'])
    logger.info("ES Push Complete")
    filelog = open(logpath, 'r')
    filecontent = filelog.read()
    mailsubject="ebay daily load complete"
    url = "http://production12.getpriceapp.com/data/notify/"
    d= {"body":str(filecontent),
    "subject":str(mailsubject)}
    r = requests.post(url,data=d)
    sys.exit(0)


