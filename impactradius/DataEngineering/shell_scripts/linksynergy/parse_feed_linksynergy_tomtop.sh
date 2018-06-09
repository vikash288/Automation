#!/bin/bash

OPTIND=1         

input_file=""
input_dir=""
retailer=""

while getopts "d:f:r:" opt; do
    case "$opt" in
    d)
        input_dir=$OPTARG
        ;;
    f)  input_file=$OPTARG
        ;;
    r)  retailer=$OPTARG
        ;;
    esac
done
shift $((OPTIND-1))


parse_file()
{
  >&2 echo "parsing $1, $2"
  python parse_feed_linksynergy_tomtop.py $1 > /home/ubuntu/affiliate/linksynergy/data/$2_$$.tsv 2>> log/$2.err

  echo "LOAD DATA LOCAL INFILE '/home/ubuntu/affiliate/linksynergy/data/$2_$$.tsv' REPLACE INTO TABLE crawler.${2}_feed FIELDS TERMINATED BY '\t'" | mysql -u crawler -h crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com -P 3306 -pprice123

  #rm /home/ubuntu/affiliate/data/$2_$$.tsv
}

echo "create table crawler.${retailer}_feed like affiliate.walmart_feed;" | mysql -u crawler -h crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com -P 3306 -pprice123

if [ -n "$input_dir" ]
then
  ls $input_dir | grep -v 'category_list' > data/${retailer}_$$.txt

  while read file_name
  do
    parse_file $input_dir/$file_name $retailer
  done < data/${retailer}_$$.txt 
  
  rm data/${retailer}_$$.txt
else
    parse_file $input_file $retailer
fi


