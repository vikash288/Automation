__author__ = 'abhinandan'

# -*- coding: utf-8 -*-

import urllib
import requests
import datetime

filelog = open('/home/ubuntu/log/automationlog/ebay/ebay_2017-08-16.log', 'r')
filecontent = filelog.read()
print(urllib.quote(filecontent.encode('utf-8')))
#print(filecontent)
mailsubject="ebay daily load complete"
r = requests.get("http://production12.getpriceapp.com/data/notify/?body="+urllib.quote(filecontent.encode('utf-8'))+"&subject="+urllib.quote(mailsubject.encode('utf-8')))
#print(r.content)
