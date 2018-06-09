
import subprocess

target_table_name = "PRICE_DOT_COM_TRGT.TGT_EBAY_USED_ITEMS"
jarpath="/home/ubuntu/ebaypush/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"


subprocess.call(['java', '-jar', '-Xmx2024m', jarpath, 'ebay-multi', target_table_name, target_table_name, 'ebay'])

