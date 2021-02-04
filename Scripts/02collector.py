# This file will extract holidays data for each country in the entered year
import json
import sqlite3
import urllib.request, urllib.parse, urllib.error
import ssl
import time

api_key = input ('Enter your api key: ')
year = input ('Enter year: ')
serviceurl = 'https://calendarific.com/api/v2/holidays?'

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('Holidays.sqlite')
cur = conn.cursor()

conn1 = sqlite3.connect('raw.sqlite')
cur1 = conn1.cursor()
cur1.execute('''
CREATE TABLE IF NOT EXISTS rawHolidays (country_id INTEGER, year TEXT, data TEXT)''')


sqlstr = '''SELECT id, country_code FROM Country ORDER BY id'''

parms = {}
parms['api_key'] = api_key
parms['year'] = year

counter = 1
for row in cur.execute(sqlstr):
    
    cid = row[0]
    c_code = row[1].strip()
    cur1.execute("SELECT data FROM rawHolidays WHERE country_id= ? AND year=?",
        (cid, year))
    try:
        data = cur1.fetchone()[0]
        continue
    except:
        pass
    
    parms["country"] = c_code
    url = serviceurl + urllib.parse.urlencode(parms)
    req = urllib.request.Request(url, headers={'User-Agent': 'XYZ/3.0'})
    connection = urllib.request.urlopen(req, context=ctx)
    data = connection.read().decode()
    
    try:
        js = json.loads(data)
    except:
        continue
    
    if js['meta']['code'] == 200 :
        cur1.execute('''INSERT INTO rawHolidays (country_id, year, data)
        VALUES ( ?, ?, ? )''', (cid, year, data ) )
        conn1.commit()
        print (counter , 'Writing data of', c_code, 'to the database')    
    else:
        print('==== Failure To Retrieve ====')
        continue
    time.sleep(1)
    counter = counter+1
cur1.close()
cur.close()
print ('\nWriting data of' , counter-1, 'countries for year of',year, 'is finished!\n' )