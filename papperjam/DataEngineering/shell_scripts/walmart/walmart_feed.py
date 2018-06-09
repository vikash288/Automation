#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('|');
  url1 = splits[5].split('murl=')[1]
  url1 = urllib.unquote(url1)

  url2 = splits[6]

  return (splits[0], splits[1], splits[2], url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)

  for line in f:
    line = line.strip()
    (wmt_id, title, upc, product_url, img_url) = parse_walmart_feed(line)

    print "%s\t%s\t%s\t%s\t%s" %(wmt_id, title, upc, product_url, img_url)
