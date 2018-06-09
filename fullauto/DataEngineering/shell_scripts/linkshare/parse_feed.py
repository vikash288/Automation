#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('|');
  url1 = splits[5].split('murl=')[1]
  url2 = urllib.unquote(url1)

  url3 = splits[6]
  upc = splits[2]
  return (splits[0], splits[1], upc, url1, url2, url3)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)

  for i, line in enumerate(f):
    if i==0:continue
    try:
      line = line.strip()
      if 'TRL' in line:break
      (wmt_id, title, upc, affiliate_url, product_url, img_url) = parse_walmart_feed(line)

      print "%s\t%s\t%s\t%s\t%s\t%s" %(wmt_id, title, upc, product_url, img_url, affiliate_url)
    except Exception as e:
      print >> sys.stderr, "error parsing line: " + line
      pass
