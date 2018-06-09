#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('|');
  url1 = splits[5].split('murl=')[1]
  url1 = urllib.unquote(url1)

  url2 = splits[6]

  return (splits[0], splits[1], splits[23], url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)
  #f1 = f
  #all_lines = f1.readlines()
  #print 'ffff -- ',f
  for i,line in enumerate(f):
    #print 'i -- ',i
    if i==0:continue
    try:
      line = line.strip()
      #print 'lineeee ',line
      #print 'last ',lines[-1].strip()
      if 'TRL' in line:continue
      (wmt_id, title, upc, product_url, img_url) = parse_walmart_feed(line)

      print "%s\t%s\t%s\t%s\t%s" %(wmt_id, title, upc, product_url, img_url)
    except Exception as e:
      print >> sys.stderr, "error parsing line: " + line
      pass
