import time
import datetime
import ConfigParser
import os
import logging

from smap.archiver.client import SmapClient
import MySQLdb

import pytz


fileDir = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig(filename= os.path.join(fileDir,'getRFID.log'), format='%(asctime)s %(message)s', level=logging.DEBUG)

Config = ConfigParser.ConfigParser()
#  look for config file in same directory as executable .py file.
Config.read(os.path.join(fileDir, 'EnergyPortal.cnf'))

# Configure DB connection
HOST = Config.get("MySQL", 'Host')
PORT = Config.getint("MySQL", 'Port')
USER = Config.get("MySQL", 'User')
PASS = Config.get("MySQL", 'Pass')
DB = Config.get("MySQL", 'DB')

#number of days to retrieve RFID history
days = Config.getint("sMAP", 'Days');

logging.info('Set up database connection')

# Connect to database
dbconn = MySQLdb.connect(host=HOST, port=PORT, user=USER, passwd=PASS, db=DB, charset="utf8")
cursor = dbconn.cursor()
cursor.execute("SET NAMES utf8")
cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
cursor.execute("SET CHARACTER_SET_RESULTS=utf8")
dbconn.commit()

# sMAP requires timestamp representations as integers or the data queries hang
end = int(time.time())
start = end - (days * (24 * 60 * 60))

# create a client object pointing to sMAP server
client = SmapClient(Config.get("sMAP", 'Host'))

deviceDef = Config.get("sMAP", 'DeviceDefinition')

# get all the metadata for all devices under mifare_rfid
tags = client.tags("Metadata/Instrument/DeviceDefinition = " + deviceDef)

logging.info('Got Tag Metadata back')

for tag in tags:
    rfid = tag["Metadata/Instrument/ID"]
    reader = tag["Metadata/Instrument/FeedName"]
    myuuid = tag["uuid"]

    # Insert into the mapping table that contains rfid to uuid map.
    # if we encounter a duplicate value update the RFID tag (probably the same)
    cursor.execute(
        """INSERT INTO building.RFIDtoUUID (RFIDTag, Reader, UUID) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE RFIDTag = RFIDTag""",
        (rfid, reader, myuuid))

theUuid, data = client.data("Metadata/Instrument/DeviceDefinition = " + deviceDef, start, end)

logging.info('Got time series data for RFID devices.')


# we should now have a numpy arrary of data elements
# that correspond to the uuid's in the uuid list

for x in range(len(theUuid)):
    myuuid = theUuid[x]
    for y in range(len(data[x])):
        tp = datetime.datetime.utcfromtimestamp(data[x][y][0] / 1000)
        # this time point is in UTC, but it doesn't know it
        utc = pytz.utc
        utc_tp = utc.localize(tp)
        # now it does, but we want pacific.
        pacific = pytz.timezone('US/Pacific')
        pac_tp = utc_tp.astimezone(pacific)
        vl = data[x][y][1]
        # note that MySQL doesn't hold timezone information in date time fields, so that is being lost on insert, hence the warning.
        # TODO figure out how truncate timezone information since MySQL wont eat it.
        cursor.execute(
            """INSERT INTO building.sMAPTimeSeries (UUID, Time, Value) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE value = value""",
            (myuuid, pac_tp, vl))

dbconn.commit()
dbconn.close()
logging.info('Finished.')
