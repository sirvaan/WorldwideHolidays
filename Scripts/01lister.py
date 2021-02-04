# This file get a list of countries that supported by the api

import json
import urllib.request, urllib.parse, urllib.error
import ssl
import sqlite3

api_key = input ('Enter your api key: ')
serviceurl = 'https://calendarific.com/api/v2/countries?'

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Prepare url as per API documentation and open a connection
parms = dict()
parms['api_key'] = api_key
url = serviceurl + urllib.parse.urlencode(parms)
req = urllib.request.Request(url, headers={'User-Agent': 'XYZ/3.0'})
connection = urllib.request.urlopen(req, context=ctx)
data = connection.read().decode()

# Read the retrieved data as json
js = json.loads(data)

# Check if the retrieved data is healthy and create a json file as a back-up
if js["meta"]['code'] == 200 :
    print('==== Failure To Retrieve ====')
    quit()

# Create a sql database and a table
conn = sqlite3.connect('Holidays.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Country
    (id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, country_name TEXT UNIQUE,
     country_code TEXT UNIQUE, total_holidays INTEGER)''')


countries = js["response"]["countries"]

# Insert extracted data to the table
for con in countries:
    cname = con['country_name']
    th = int (con['total_holidays'])
    ccode = con['iso-3166']
    cur.execute('INSERT OR IGNORE INTO Country (country_name, country_code, total_holidays) VALUES ( ?, ?, ? )',
    ( cname, ccode, th ) )
    conn.commit()





