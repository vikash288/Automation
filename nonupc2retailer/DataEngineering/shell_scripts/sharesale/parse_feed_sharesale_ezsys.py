#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('|');
  url1 = splits[4]
  url1 = urllib.unquote(url1)
  url2 = splits[5]
  return (splits[12], splits[11], url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)

  for line in f:
    try:
        line = line.strip()
        (wmt_id, title, product_url, img_url) = parse_walmart_feed(line)

        print "%s\t%s\t%s\t%s\t%s" %(wmt_id, title,'', product_url, img_url)
    except Exception as e:
        print >> sys.stderr, "error parsing line: " + line + str(e)
        pass
