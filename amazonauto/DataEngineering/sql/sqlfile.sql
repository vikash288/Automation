create table PRICE_DOT_COM_TRGT.TRGT_OTTERBOX_CJ as select * from PRICE_DOT_COM_TRGT.TRGT_FINISH_LINE_LINKSHARE_VIKASH limit 0;

create table STG_SPIRITHALLOWEENCOM_PEPPERJAM_W as select * from STG_JOHNSONMURPHY_LINKSHARE_W limit 0;

/////Cj///////

Insert into UPC_COMMON_ID_MAP2
 Select TGT.COMMON_ID, SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID from
(SELECT
    FIELD_11 AS UPC,
    39 AS RETAILER_CODE,
    FIELD_8 AS ITEM_ID
  FROM STG_EASYSPIRIT_CJ_W 
  WHERE FIELD_11 <> 'UPC' AND FIELD_11 IS NOT NULL AND FIELD_11 <> ' '
  GROUP BY UPC, RETAILER_CODE
) SRC
JOIN UPC_COMMON_ID_MAP2 TGT
ON SRC.UPC = TGT.UPC
and TGT.RETAILER_CODE <> 39
group by 1,2,3,4
;

Truncate UPC_COMMON_ID_MAP_W3;

Insert into UPC_COMMON_ID_MAP_W3 (
  UPC,
  RETAILER_CODE,
  ITEM_ID
)
 Select SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID from
(SELECT
    FIELD_11 AS UPC,
    39 AS RETAILER_CODE,
    FIELD_8 AS ITEM_ID
  FROM STG_EASYSPIRIT_CJ_W
  WHERE FIELD_11 <> 'UPC' AND FIELD_11 IS NOT NULL AND FIELD_11 <> ' '
 GROUP BY UPC, RETAILER_CODE
) SRC
LEFT JOIN UPC_COMMON_ID_MAP2 TGT
ON SRC.UPC = TGT.UPC
where TGT.UPC is null
;

Insert into UPC_COMMON_ID_MAP2
Select common_id + (Select max(common_id) from UPC_COMMON_ID_MAP2), upc, retailer_code, ITEM_ID
from UPC_COMMON_ID_MAP_W3;

 

insert into PRICE_DOT_COM_TRGT.TRGT_KMART_CJ


create table PRICE_DOT_COM_TRGT.TRGT_EASYSPIRIT_CJ
select
  a.FIELD_8 as ITEM_ID,
  a.FIELD_5 as TITLE,
  a.FIELD_11 as UPC,
  a.FIELD_18 as PRODUCT_URL,
  a.FIELD_20 as IMAGE_URL,
  a.FIELD_18 as AFFILIATE_URL,
  b.common_id as PRICE_ID,
  a.FIELD_14 + ' ' AS PRICE1,
  a.FIELD_15 + ' ' AS PRICE2
from STG_EASYSPIRIT_CJ_W a, UPC_COMMON_ID_MAP2 b
where a.FIELD_11 = b.upc
and b.retailer_code = 39
and a.FIELD_11 <> 'UPC'
and a.FIELD_11 is not null
and a.FIELD_11 <> ' '
and length(a.FIELD_11) < 51 





//////LINKSHARE///////


Insert into UPC_COMMON_ID_MAP2
 Select TGT.COMMON_ID, SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID from
(SELECT
    FIELD_24 AS UPC,
    12 AS RETAILER_CODE,
    FIELD_1 AS ITEM_ID
  FROM STG_BLOOMINGDALE_LINKSHARE_W
  WHERE FIELD_24 <> 'UPC' AND FIELD_24 IS NOT NULL AND FIELD_24 <> ' '
  GROUP BY UPC, RETAILER_CODE
) SRC
JOIN UPC_COMMON_ID_MAP2 TGT
ON SRC.UPC = TGT.UPC
and TGT.RETAILER_CODE <> 12
group by 1,2,3,4
;

Truncate UPC_COMMON_ID_MAP_W3;


Insert into UPC_COMMON_ID_MAP_W3 (
  UPC,
  RETAILER_CODE,
  ITEM_ID
)
 Select SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID from
(SELECT
    FIELD_24 AS UPC,
    12 AS RETAILER_CODE,
    FIELD_1 AS ITEM_ID
  FROM STG_BLOOMINGDALE_LINKSHARE_W
  WHERE FIELD_24 <> 'UPC' AND FIELD_24 IS NOT NULL AND FIELD_24 <> ' '
 GROUP BY UPC, RETAILER_CODE
) SRC
LEFT JOIN UPC_COMMON_ID_MAP2 TGT
ON SRC.UPC = TGT.UPC
where TGT.UPC is null
;


Insert into UPC_COMMON_ID_MAP2
Select common_id + (Select max(common_id) from UPC_COMMON_ID_MAP2), upc, retailer_code, ITEM_ID
from UPC_COMMON_ID_MAP_W3;

 
create table PRICE_DOT_COM_TRGT.TRGT_BLOOMINGDALE_LINKSHARE
select
  a.FIELD_1 as ITEM_ID,
  a.FIELD_2 as TITLE,
  a.FIELD_24 as UPC,
  a.FIELD_6 as PRODUCT_URL,
  a.FIELD_7 as IMAGE_URL,
  a.FIELD_8 as AFFILIATE_URL,
  b.common_id as PRICE_ID,
  a.FIELD_13 + ' ' AS PRICE1,
  a.FIELD_14 + ' ' AS PRICE2
from STG_BLOOMINGDALE_LINKSHARE_W a, UPC_COMMON_ID_MAP2 b
where a.FIELD_24 = b.upc
and b.retailer_code = 12
and a.FIELD_24 <> 'UPC'
and a.FIELD_24 is not null
and a.FIELD_24 <> ' '
and length(a.FIELD_24) < 51 ;

///////Validate PRICE_ID///

select count(UPC_COMMON_ID_MAP2.common_id), count(UPC_COMMON_ID_MAP2.UPC) 
from UPC_COMMON_ID_MAP2, STG_EASYSPIRIT_CJ_W 
where  STG_EASYSPIRIT_CJ_W.FIELD_11 =UPC_COMMON_ID_MAP2.upc and 
UPC_COMMON_ID_MAP2.retailer_code=39

select STG_EASYSPIRIT_CJ_W.FIELD_11 , count(UPC_COMMON_ID_MAP2.common_id)
from UPC_COMMON_ID_MAP2, STG_EASYSPIRIT_CJ_W 
where  STG_EASYSPIRIT_CJ_W.FIELD_11 =UPC_COMMON_ID_MAP2.upc and 
UPC_COMMON_ID_MAP2.retailer_code=39
group by 1
having count(UPC_COMMON_ID_MAP2.common_id) > 1


select STG_EASYSPIRIT_CJ_W.FIELD_11 , count(UPC_COMMON_ID_MAP2.retailer_code)
from UPC_COMMON_ID_MAP2, STG_EASYSPIRIT_CJ_W 
where  STG_EASYSPIRIT_CJ_W.FIELD_11 =UPC_COMMON_ID_MAP2.upc
group by 1
having count(UPC_COMMON_ID_MAP2.common_id) > 1
