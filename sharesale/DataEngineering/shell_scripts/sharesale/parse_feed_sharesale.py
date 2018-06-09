#!/usr/bin/python
import urllib
import sys

def parse_walmart_feed(line):
  splits = line.split('|');
  url1 = splits[4]
  #url1 = splits[8] #for dealdatabase
  url1 = urllib.unquote(url1) 
  url2 = splits[5]
  #url2 = splits[7] #for dealdatabase 
  if url2=='' or url2==None:
      url2 = splits[6]
      #url2 = splits[9] # for dealdatabase feed
  upc = splits[24] #for the_nerds
  if upc=='' or upc==None:
      upc = splits[25] #for apparelnbags,gliks
      if upc == '' or upc == None:
          upc = splits[26] # for royalrobbins feed
  if len(upc) == 12:
      upc = upc
  elif upc[0] == '0' and len(upc) == 13:
      upc = upc
  else:
      upc = ''
  title = splits[1]
  #title = splits[2] #for dealdatabase feed
  #upc = '' #for choies
  return (splits[0], title, upc, url1, url2)

if __name__ == "__main__":
  file_name = sys.argv[1]
  f = open(file_name)
  for i, line in enumerate(f):
    #if i<=4:continue  #for choies feed
    #if i==10:break
    try:
        line = line.strip()
        (wmt_id, title, upc, product_url, img_url) = parse_walmart_feed(line)

        print "%s\t%s\t%s\t%s\t%s" %(wmt_id, title, upc, product_url, img_url)
    except Exception as e:
        print >> sys.stderr, "error parsing line: " + line + str(e)
        pass
