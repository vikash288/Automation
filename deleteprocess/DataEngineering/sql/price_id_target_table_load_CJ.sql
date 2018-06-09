{\rtf1\ansi\ansicpg1252\cocoartf1265\cocoasubrtf210
{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red180\green36\blue25;\red200\green20\blue201;\red64\green11\blue217;
\red193\green101\blue28;\red46\green174\blue187;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx560\tx1120\tx1680\tx2240\tx2800\tx3360\tx3920\tx4480\tx5040\tx5600\tx6160\tx6720\pardirnatural

\f0\fs22 \cf0 \CocoaLigature0 \'97- $s3cmd get s\cf2 3\cf0 ://price-data-engineering/2017-02\cf2 -19\cf0 /Walmart/* .\
\
use production;\
load data local infile \cf2 '~/s3upload/DataEngineering/data/Johnston&Murphy/42258_3310515_107901534_cmp.txt'\cf0  \cf3 into\cf0  \cf3 table\cf0  STG_JOHNSONMURPHY_LINKSHARE_W fields terminated \cf3 by\cf0  \cf2 '|'\cf0  ;\cf4 \
\
----when matches\cf0 \
\
\cf4 --- create table UPC_COMMON_ID_MAP2_date as select * from UPC_COMMON_ID_MAP2;\cf0 \
\
\cf4 --- Never ever truncate or drop UPC_COMMON_ID_MAP2 table\cf0 \
\cf4 -- change the staging tale name\cf0 \
\cf4 --- Insert for matching\cf0 \
\
\cf5 Insert\cf0  \cf3 into\cf0  UPC_COMMON_ID_MAP2\
 \cf5 Select\cf0  TGT.COMMON_ID, SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID \cf3 from\cf0 \
(\cf5 SELECT\cf0 \
    FIELD_11 \cf3 AS\cf0  UPC,\
    \cf2 13\cf0  \cf3 AS\cf0  RETAILER_CODE,\
    FIELD_8 \cf3 AS\cf0  ITEM_ID\
  \cf3 FROM\cf0  STG_FINISHLINE_LINKSHARE_W_VIKASH\
  \cf3 WHERE\cf0  FIELD_24 <> \cf2 'UPC'\cf0  \cf5 AND\cf0  FIELD_24 \cf3 IS\cf0  \cf5 NOT\cf0  \cf3 NULL\cf0  \cf5 AND\cf0  FIELD_24 <> \cf2 ' '\cf0 \
  \cf3 GROUP\cf0  \cf3 BY\cf0  UPC, RETAILER_CODE\
) SRC\
JOIN UPC_COMMON_ID_MAP2 TGT\
\cf3 ON\cf0  SRC.UPC = TGT.UPC\
\cf5 and\cf0  TGT.RETAILER_CODE <> \cf2 13\cf0 \
;\
\
\cf4 ---- prepare for non maching UPC \cf0 \
\cf4 -- change the staging tale name\cf0 \
\cf4 -- change the retailer code : IMPORTANT\cf0 \
\
\cf5 Truncate\cf0  UPC_COMMON_ID_MAP_W3;\
\
\
\cf5 Insert\cf0  \cf3 into\cf0  UPC_COMMON_ID_MAP_W3 (\
  UPC,\
  RETAILER_CODE,\
  ITEM_ID\
)\
 \cf5 Select\cf0  SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID \cf3 from\cf0 \
(\cf5 SELECT\cf0 \
    FIELD_11 \cf3 AS\cf0  UPC,\
    \cf2 13\cf0  \cf3 AS\cf0  RETAILER_CODE,\
    FIELD_8 \cf3 AS\cf0  ITEM_ID\
  \cf3 FROM\cf0  STG_FINISHLINE_LINKSHARE_W_VIKASH\
  \cf3 WHERE\cf0  FIELD_24 <> \cf2 'UPC'\cf0  \cf5 AND\cf0  FIELD_24 \cf3 IS\cf0  \cf5 NOT\cf0  \cf3 NULL\cf0  \cf5 AND\cf0  FIELD_24 <> \cf2 ' '\cf0 \
  \cf3 GROUP\cf0  \cf3 BY\cf0  UPC, RETAILER_CODE\
) SRC\
LEFT JOIN UPC_COMMON_ID_MAP2 TGT\
\cf3 ON\cf0  SRC.UPC = TGT.UPC\
\cf3 where\cf0  TGT.UPC \cf3 is\cf0  \cf3 null\cf0 \
;\
\
\
\
\cf5 Insert\cf0  \cf3 into\cf0  UPC_COMMON_ID_MAP2\
\cf5 Select\cf0  common_id + (\cf5 Select\cf0  \cf6 max\cf0 (common_id) \cf3 from\cf0  UPC_COMMON_ID_MAP2), upc, retailer_code, ITEM_ID\
\cf3 from\cf0  UPC_COMMON_ID_MAP_W3\
\
\cf5 commit\cf0 ;\
\
\
==== target \cf3 table\cf0  load====\
\cf4 -- CJ INSERTS\cf0 \
\
\
\cf4 ---truncate table PRICE_DOT_COM_TRGT.TRGT_FINISH_LINE_LINKSHARE;\cf0 \
\
\cf5 insert\cf0  \cf3 into\cf0  PRICE_DOT_COM_TRGT.TRGT_FINISH_LINE_LINKSHARE\
\cf5 select\cf0 \
  a.FIELD_8 \cf3 as\cf0  ITEM_ID,\
  a.FIELD_5 \cf3 as\cf0  TITLE,\
  a.FIELD_11 \cf3 as\cf0  UPC,\
  SUBSTRING_INDEX(a.FIELD_18, \cf2 'url='\cf0 , \cf2 -1\cf0 ) \cf3 as\cf0  PRODUCT_URL,\
  a.FIELD_20 \cf3 as\cf0  IMAGE_URL,\
  b.common_id \cf3 as\cf0  PRICE_ID,\
  a.FIELD_14 \cf3 AS\cf0  PRICE1,\
  a.FIELD_15 \cf3 AS\cf0  PRICE2\
\cf3 from\cf0  STG_FINISHLINE_CJ_W_VIKASH a, UPC_COMMON_ID_MAP2 b\
\cf3 where\cf0  a.FIELD_11 = b.upc\
\cf5 and\cf0  b.retailer_code = \cf2 5\cf0 \
\cf5 and\cf0  a.FIELD_11 <> \cf2 'UPC'\cf0 \
\cf5 and\cf0  a.FIELD_11 \cf3 is\cf0  \cf5 not\cf0  \cf3 null\cf0 \
\cf5 and\cf0  a.FIELD_11 <> \cf2 ' '\cf0 \
\cf5 and\cf0  \cf6 length\cf0 (a.FIELD_11) < \cf2 51\cf0 \
;}