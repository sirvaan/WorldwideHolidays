# This file will extract and clean raw data

import sqlite3
import json


conn = sqlite3.connect('raw.sqlite')
cur = conn.cursor()
sqlsrt = 'SELECT * FROM rawHolidays order by country_id'

conn1 = sqlite3.connect('Holidays.sqlite')
cur1 = conn1.cursor()
cur1.execute('''CREATE TABLE IF NOT EXISTS holidays
    (id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, name TEXT,description TEXT,
     country_id TEXT ,full_date TEXT, year INTEGER, month INTEGER, day INTEGER, type TEXT, location TEXT)''')
counter = 1
for row in cur.execute(sqlsrt):
    
    c_id = row[0]
    year = row[1]
    data = row[2]
    js = json.loads(data)
    if js['meta']['code'] != 200 : continue
    for holi in js ['response']['holidays']:
        name = holi['name']
        desc = holi ['description']
        mon = holi['date']['datetime']['month']
        day = holi['date']['datetime']['day']
        typ = holi['type'][0]
        loc = holi ['locations']
        fdate = holi['date']['iso'][:10]
        cur1.execute ('SELECT * FROM Holidays WHERE country_id = ? AND full_date=?',(c_id, fdate))
        try:
            da = cur1.fetchone()[0]
            print ('Holiday for', holi['country']['name'], 'in', fdate, 'is exist!')
            continue
        except:
            pass
        cur1.execute('''INSERT INTO holidays (name ,description,
        country_id , year , month , day, full_date, type, location)
        VALUES ( ?, ?, ?, ?, ? , ?, ?, ?, ? )''', ( name, desc, c_id, year,mon,day, fdate, typ,loc ) )
        print (counter , 'Inserting new row for', holi['country']['name'], 'in', fdate, '...')
        counter = counter + 1
        conn1.commit()
print ('\n', counter-1, 'rows added!\n')

cur1.close()
cur.close()