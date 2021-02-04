# This file will export data to a csv file

import sqlite3
import csv
import codecs

conn = sqlite3.connect('Holidays.sqlite')
cur = conn.cursor()
cur.execute ('''SELECT holidays.id, holidays.name, holidays.description, Country.country_code,Country.country_name ,
            holidays.year, holidays.month, holidays.day, holidays.type,holidays.location, Country.country_code,Country.country_name 
            FROM holidays JOIN Country ON holidays.country_id = Country.id ORDER BY Country.country_name''')

with codecs.open("out.csv", "w","utf-8") as csv_file: 
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cur.description]) # write headers
    csv_writer.writerows(cur)