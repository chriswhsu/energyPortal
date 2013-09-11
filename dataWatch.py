__author__ = 'chriswhsu'

import ConfigParser
import smtplib
from email.mime.text import MIMEText
import os

import MySQLdb
from html import HTML


Config = ConfigParser.ConfigParser()

#  look for config file in same directory as executable .py file.
Config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'EnergyPortal.cnf'))

# Connect to database
HOST = Config.get("MySQL", 'Host')
PORT = Config.getint("MySQL", 'Port')
USER = Config.get("MySQL", 'User')
PASS = Config.get("MySQL", 'Pass')
DB = Config.get("MySQL", 'DB')

dbconn = MySQLdb.connect(host=HOST, port=PORT, user=USER, passwd=PASS, db=DB, charset="utf8")
cursor = dbconn.cursor()
cursor.execute("SET NAMES utf8")
cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
cursor.execute("SET CHARACTER_SET_RESULTS=utf8")
dbconn.commit()

cursor.execute("""SELECT 1                          AS seqno,
                       Round_timestamp(time, 300) AS time_inverval,
                       id,
                       Min(energy)                start_energy,
                       Max(energy)                end_energy,
                       Count(*)                   AS row_count
                FROM   SmartMeter USE KEY(time)
                WHERE  time > Subtime(Round_timestamp(Now(), 300), '00:15:00')
                   AND time <= Subtime(Round_timestamp(Now(), 300), '00:10:00')
                GROUP  BY 3,
                          2,
                          1
                UNION ALL
                SELECT 2                          AS seqno,
                       Round_timestamp(time, 300) AS time_interval,
                       id,
                       Min(energy)                start_energy,
                       Max(energy)                end_energy,
                       Count(*)                   AS row_count
                FROM   SmartMeter USE KEY(time)
                WHERE  time > Subtime(Round_timestamp(Now(), 300), '00:10:00')
                   AND time <= Subtime(Round_timestamp(Now(), 300), '00:05:00')
                GROUP  BY 3,
                          2,
                          1
                ORDER  BY 3,
                          2,
                          1
                          """)

data = cursor.fetchall()

lastCount = 0
lastMeterID = ''
lastMax = 0
pairComplete = True
sendEmail = False
theoutput = HTML()
theoutput.style('td', '{font-size:50%}')
table = theoutput.table(border='2')
r = table.tr()
r.th.center.b('Meter')
r.th.center.b('Time Interval')
r.th.center.b('Row Count')
r.th.center.b('Min Energy')
r.th.center.b('Max Energy')
r.th.center.b('Issue')

for row in data:
    seqNo = row[0]
    timeInterval = row[1]
    meterID = row[2]
    minEnergy = row[3]
    maxEnergy = row[4]
    rowCount = row[5]
    r = table.tr

    if seqNo == 1:

        if not pairComplete:
            sendEmail = True
            r.td.b(str(lastMeterID) + ': sensor dropped.', style="color:red")
            r.td('')
            r.td('')
            r.td('')
            r.td('')
            r = table.tr
            r = table.tr
            r = table.tr

        r.td(str(meterID))
        r.td(str(timeInterval))
        r.td(str(rowCount))
        r.td(str(minEnergy))
        r.td(str(maxEnergy))
        pairComplete = False

    elif seqNo == 2:
        if lastMeterID != meterID:  # new feed
            sendEmail = True
            r.td.b(str(meterID) + ': new sensor feed', style="color:green")
            r.td('')
            r.td('')
            r.td('')
            r.td('')
            r = table.tr

        r.td(str(meterID))
        r.td(str(timeInterval))
        r.td(str(rowCount))
        r.td(str(minEnergy))
        r.td(str(maxEnergy))

        if lastMeterID == meterID:

            # if less than 80% of the rows of the previous time period, raise an alert
            if rowCount < lastCount * Config.getfloat('ACME', 'tolerance'):
                sendEmail = True
                r.td.b('drop in counts', style="color:red")

            # did the net energy drop by more than a certain amount
            if lastMax - minEnergy > Config.getint('ACME', 'energyTolerance'):
                sendEmail = True
                r.td.b('drop in net energy', style="color:red")
        r = table.tr
        r = table.tr
        pairComplete = True  # pair complete or new meter.

    lastMeterID = meterID
    lastCount = rowCount
    lastMax = maxEnergy

if sendEmail:
    print('need to notify someone')
    msg = MIMEText(str(theoutput), 'html')
    msg['Subject'] = 'Alert: Plug-Meter Data Anomaly'
    msg['From'] = Config.get("EMAIL", 'mailFrom')
    msg['To'] = Config.get("EMAIL", 'mailTo')

    try:

        smtpObj = smtplib.SMTP(Config.get("EMAIL", 'smtpServer'), Config.getint("EMAIL", 'port'))
        smtpObj.sendmail(Config.get("EMAIL", 'mailFrom'), [Config.get("EMAIL", 'mailTo')], msg.as_string())
        #print theoutput
        print "Successfully sent email"
    except smtplib.SMTPException:
        print "Error: unable to send email"
else:
    print('No email needed.')

dbconn.close()
