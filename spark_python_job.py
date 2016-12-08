%python
import yahoo_finance
from yahoo_finance import Share
from cassandra.cluster import Cluster


print "Hello World"

try:
    c=Cluster(['Add Cassandra IP'])  
    session = c.connect()
    print 'Able to connect to cassandra'
    
    session.execute('USE cmpe274')
    #resultset = session.execute('select * from share')
    #resultset.map(lambda p: Row(close=p.close))
    #resultset.show()
    
  
except Exception as e:
    print e
