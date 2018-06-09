#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split(',');
  url1 = splits[9]
  url1 = urllib.unquote(url1)
  #print 'urll1 ',url1
  url2 = splits[26]
  #print 'url2 ',url2
  upc = splits[69]
  #print 'upc ',upc
  #print 'sku ',splits[65]
  #print 'title ',splits[42]
  return (splits[65], splits[42], upc, url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)

  for i, line in enumerate(f):
    #print i
    if i == 0:continue
    try:
        line = line.strip()
        (wmt_id, title, upc, product_url, img_url) = parse_walmart_feed(line)

        print "%s\t%s\t%s\t%s\t%s" %(wmt_id, title, upc, product_url, img_url)
    except Exception as e:
        print >> sys.stderr, "error parsing line: " + line + str(e)
        pass
