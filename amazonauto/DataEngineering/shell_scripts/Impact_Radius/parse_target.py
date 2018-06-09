#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('\t');
  item_id = splits[0]
  title = splits[1]
  upc = splits[13]
  if upc == None or upc == '':
    upc = splits[14]

  url1 = splits[7]
  if url1 == None or url1 == '':
    url1 = splits[8]

  url_split = url1.split('&u=')
  if len(url_split) > 1:
    url1 = url_split[1]
  url1 = urllib.unquote(url1)

  url2 = urllib.unquote(splits[9])
  if url2 == None or url2 == '':
    url2 = urllib.unquote(splits[10])

  return (item_id, title, upc, url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)

  for i,line in enumerate(f):
    if i == 0:
        continue
    try:
        line = line.strip()
        (item_id, title, upc, product_url, img_url) = parse_walmart_feed(line)
        if item_id == 'id': #a bit of a hack
          continue

        print "%s\t%s\t%s\t%s\t%s" %(item_id, title, upc, product_url, img_url)
    except Exception as e:
        print >> sys.stderr, "error parsing line: " + line + str(e)
        pass
