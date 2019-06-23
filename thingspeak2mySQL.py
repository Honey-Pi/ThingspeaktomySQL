#!/usr/local/bin/python3

import thingspeak
import json
import mysql.connector as mariadb

result={}
mariadb_connection = mariadb.connect(user='', password='', host='', database='')
cursor = mariadb_connection.cursor()
cursor.execute("SELECT * FROM HUM ORDER BY ID DESC LIMIT 1")
records = cursor.fetchall()
records = list(records[0])
records=records[2]

ch = thingspeak.Channel(757628)

data1 = ch.get_field(field=1)
data2 = ch.get_field(field=2)
data3 = ch.get_field(field=3)

parsed1 = json.loads(data1)
parsed2 = json.loads(data2)
parsed3 = json.loads(data3)

data1 = parsed1['feeds']
data2 = parsed2['feeds']
data3 = parsed3['feeds']

for i in data1:
    a = i['entry_id']
    b = i['created_at']

    if a <= records:
        pass
    else:
        if i['field1'] is not None:
            result[a] = float(i['field1'])
            cursor.execute("INSERT INTO Gewicht (TIMESTAMP, KG) VALUES (%s,%s)",(b, result[a]))
            mariadb_connection.commit()
        else:
            pass

for i in data2:
    a = int(i['entry_id'])
    b = i['created_at']

    if a <= records:
        pass
    else:
        if i['field2'] is not None:
            result[a] = float(i['field2'])
            cursor.execute("INSERT INTO TEMP (TIMESTAMP, TEMP) VALUES (%s,%s)",(b, result[a]))
            mariadb_connection.commit()
        else:
            pass
for i in data3:
    a = int(i['entry_id'])
    b = i['created_at']

    if a <= records:
        pass
    else:
        if i['field3'] is not None:
            result[a] = float(i['field3'])
            cursor.execute("INSERT INTO HUM (TIMESTAMP, HUM, ID) VALUES (%s,%s,%s)",(b, result[a], a))
            mariadb_connection.commit()
        else:
            pass
