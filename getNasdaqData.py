import csv
import yahoo_finance
from yahoo_finance import Share
from cassandra.cluster import Cluster
from datetime import datetime
import time

try:
	c=Cluster(['54.191.200.22'])  
	session = c.connect()
	print 'Able to connect to cassandra'

except Exception as e:
	print e

def readCSV():
	with open('sample.csv') as f:
	    reader = csv.DictReader(f) 
	    for row in reader: 
	    	for (k,v) in row.items():
	    		getData(v)

def uniqid():
    return str(round(time.time() * 1000))


def getData(symbol):
	yahoo = Share(symbol)
	try:
		data = yahoo.get_historical('2015-11-25','2016-11-25')
		print len(data)
		for record in data:
			insertData(record)

	except Exception as e:
		print e

def insertData(data):
	session.execute('USE cmpe274')
	session.execute(
    """
    INSERT INTO share (share_id, adj_close, close, date, high, low, open, symbol, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (uniqid(), data['Adj_Close'], data['Close'], data['Date'], data['High'], data['Low'], data['Open'], data['Symbol'], data['Volume'])
   )
	time.sleep(1)


def main():
	readCSV()
	


if __name__ == '__main__':
        main()