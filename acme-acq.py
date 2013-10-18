import httplib
import json
import time
import os
import datetime

import MySQLdb

meters = ['8b6','8b7','8b8','8b9','8ba','8d4','8d5','8d6','8d7','8d8','8e3','8e4','8e5','8e6','8e7','936','937','93d','93e','93f','940','941','947','948','949','94a','94b','94c','94d','94e','94f','950']
update_time = {m:0 for m in meters }

conn = httplib.HTTPConnection("192.168.0.105",8080)

# Connect to database
HOST = "128.32.33.229"
PORT = 3306
USER = "server406"
PASS = "project406"
DB = "building"

dbconn = MySQLdb.connect(host=HOST, port=PORT, user=USER, passwd=PASS, db=DB, charset="utf8")
cursor = dbconn.cursor()
cursor.execute("SET NAMES utf8")
cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
cursor.execute("SET CHARACTER_SET_RESULTS=utf8")
dbconn.commit()


while True:
 os.system('clear')
 print datetime.datetime.now()
 conn.request("GET", "/data/+")
 r = conn.getresponse()
 print r.status, r.reason
 dat_str = r.read()
 reading = json.loads(dat_str)

 for i in meters:
  v={'ap':[False,False],'tp':[False,False],'te':[False,False]}
  t={'ap':[False,False],'tp':[False,False],'te':[False,False]}

  index1 = '/costas_acmes/'+i+'/apparent_power'
  index2 = '/costas_acmes/'+i+'/true_power'
  index3 = '/costas_acmes/'+i+'/true_energy'

  if reading.has_key(index1): 
   t['ap'][0]=reading[index1]['Readings'][0][0]
   v['ap'][0]=reading[index1]['Readings'][0][1]
   t['ap'][1]=reading[index1]['Readings'][1][0]
   v['ap'][1]=reading[index1]['Readings'][1][1]

  if reading.has_key(index2):
   t['tp'][0]=reading[index2]['Readings'][0][0]
   v['tp'][0]=reading[index2]['Readings'][0][1]
   t['tp'][1]=reading[index2]['Readings'][1][0]
   v['tp'][1]=reading[index2]['Readings'][1][1]

  if reading.has_key(index3):
   t['te'][0]=reading[index3]['Readings'][0][0]
   v['te'][0]=reading[index3]['Readings'][0][1]
   t['te'][1]=reading[index3]['Readings'][1][0]
   v['te'][1]=reading[index3]['Readings'][1][1]
  

  if t['ap'][1] > update_time[i]:
   # log into file
   #filename = 'log_'+i+'.csv'
   #f=open(filename,'a')
   #line1=str(t['ap'][0]) +','+ str(v['ap'][0]) +','+ str(v['tp'][0]) +','+ str(v['te'][0]) +'\n'
   #line2=str(t['ap'][1]) +','+ str(v['ap'][1]) +','+ str(v['tp'][1]) +','+ str(v['te'][1]) +'\n'
   #f.write(line1)
   #f.write(line2)
   #f.close()
   #print i,t['ap'],t['tp'],t['te'],v['ap'],v['tp'],v['te'] 
   update_time[i]=t['ap'][1]
   # update to database
   for reading_id in range(0,2):
    ID = i
    Time = datetime.datetime.fromtimestamp(int(t['ap'][reading_id])/1000)
    Power = v['tp'][reading_id]
    ApparentPower = v['ap'][reading_id]
    Energy = v['te'][reading_id]
    print("ID=%s, Time=%s, Power=%s, AparrentPower=%s, Energy=%s" % (ID, Time, Power, ApparentPower, Energy))
    cursor.execute("""INSERT IGNORE INTO building.SmartMeter (ID, Time, Power, ApparentPower, Energy) VALUES (%s, %s, %s, %s, %s)""", (ID, Time, Power, ApparentPower, Energy))
  else:
   print i, '---'
 dbconn.commit()
 # end for loop

 time.sleep(0.8)
 # end while loop (1 sec / loop)

conn.close()

