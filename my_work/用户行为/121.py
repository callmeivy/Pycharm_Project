# coding=UTF-8
import datetime
import pymongo
import time
import MySQLdb
import math
import csv

csvfile = file('1.csv', 'wb')
writer = csv.writer(csvfile)
writer.writerow(['ip', 'date', 'caid','behave'])
conn=pymongo.Connection('172.16.168.45',27017)
iae_hitlog_new=conn.gehua.iae_hitlog_new
index = iae_hitlog_new.create_index("ip")
# ip_as=iae_hitlog_new.distinct("ip")
print 3

ind=0
ind2=0
rowNumber=0
sum=0

# for ip_a in ip_as:
cutornot=False
timelist=list()
caid=list()
resource=list()
previous=list()
length=0
lines=iae_hitlog_new.find({'ip':"10.187.3.44"}).sort('date',pymongo.ASCENDING)

 #get caid and resource
for line in lines:
    print ind2
    if ind2>10:
        break
    ind2=ind2+1
    ip="10.187.3.44"
    date=line['date']
    caid=line.get('previous').get('parameter').get('CAID')
    print caid
    tempPrevious=line.get('previous')

    writer.writerow([ip,date,caid,tempPrevious])

csvfile.close()


conn.close()
# mysqlconn1.close()