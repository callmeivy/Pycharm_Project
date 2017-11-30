#coding:UTF-8
'''
Created on 2014.6.9

@author: hao
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append('/opt/IAE')
from mfdb.refreshRedis import refresh
# log
import logging
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='channelTopN.log',
                filemode='a')
infologger = logging.getLogger("channelTopN")

# configuration
from ConfigParser import ConfigParser
try:
	config = ConfigParser()
	config.read('../config.ini')
	mongodbhost = config.get("mongodb", "ip")
	mongodbdb = config.get("mongodb", "database")
	mongodbuserinfo = config.get("mongodb", "userInfo")
	mongodbgehuarate = config.get("mongodb", "gehuarate")
	mongodbaudiencerate = config.get("mongodb", "audiencerate")
	#
	mysqlhost = config.get("mysql", "ip")
	mysqluser = config.get("mysql", "user")
	mysqlpasswd = config.get("mysql", "passwd")
	mysqldb = config.get("mysql", "database")
	mysqlchanneltopN = config.get("mysql", "channelTopN")
except Exception, e:
	infologger.error(e)
	sys.exit(1)

import time, datetime

from intermediate.getVSPData import getVSPdata

import MySQLdb
import pymongo

from bson.son import SON

class processChannelTopN():
	def __init__(self, aShiftTime):
		# mongoDB
		self.pyconn = pymongo.Connection(host=mongodbhost, port=27017)
		# mysql
		self.mysqlconn = MySQLdb.connect(host = mysqlhost, user = mysqluser, passwd = mysqlpasswd, db = mysqldb, charset = 'utf8')
		self.mysqlcursor = self.mysqlconn.cursor()
		'''
		set shift time such as 300 means 5 mins
		'''
		self.tempTime = int(time.time())
		self.tempTime = self.tempTime - self.tempTime%60 - aShiftTime
		self.createTime = datetime.datetime.now()

		self.insertDict = dict()
		self.insertList = list()
		self.insertLeft = range(1,21,1)

		self.result = list()
		
		self.mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS %s(
	    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, channel_id VARCHAR(255), channel_name VARCHAR(255), channel_pic VARCHAR(255), main_rank int(11),
	    sub_rank int(11), score bigint, systype int(11), status int(11), main_type int(11), sub_type int(11), 
	    isMainLock int(11), isSubLock int(11), create_date DATETIME) charset=utf8
	    ''' %mysqlchanneltopN)
		#get maxpk
# 		self.mysqlcursor.execute('select max(pk) from %s;' %(mysqlchanneltopN))
# 		self.maxpk = self.mysqlcursor.fetchall()[0][0]
# 		if self.maxpk is None:
# 			self.maxpk = 0

		# get maxdate
		self.mysqlcursor.execute('select max(create_date) from %s;' %(mysqlchanneltopN))
		self.maxdate = self.mysqlcursor.fetchall()[0][0]

	def __del__(self):
		self.mysqlcursor.close()
		self.mysqlconn.close()

	def readLock(self):
		#########			read the lock
		if self.maxdate is not None:
			self.mysqlcursor.execute('select * from %s where create_date="%s" and isSubLock = 1;' %(mysqlchanneltopN, self.maxdate))
			mysqlResults = self.mysqlcursor.fetchall()
			for mysqlResult in mysqlResults:
# 				self.maxpk+=1
				mysqlResult = list(mysqlResult)
# 				mysqlResult[0] = self.maxpk
				mysqlResult[8] = '0'
				self.insertList.append(mysqlResult[1])
				mysqlResult[13] = str(self.createTime)
				self.insertDict[mysqlResult[5]] = tuple(mysqlResult[1:])
				self.insertLeft.remove(mysqlResult[5])

	def freshPrevious(self):
		#set the old data status to 1
		try:
			if self.maxdate is not None:
				self.mysqlcursor.execute('update %s set status=1 where create_date="%s"' %(mysqlchanneltopN, self.maxdate))
		except:
			infologger.info("First Time!")

	def calculateRate(self):
		# calculate the rate
		reducer =	'''
					function(obj, prev){
						prev.rate+=obj.rate;
						}  
		        	'''
		# pymongoCmd = '''self.result = self.pyconn.%s.%s.group(key={"channelId":1}, condition={"tempIntTime":{"$lt":%d}}, initial={"rate": 0}, reduce=reducer)''' %(mongodbdb ,mongodbaudiencerate, self.tempTime)
		self.result = eval('''self.pyconn.%s.%s.group(key={"channelId":1}, condition={"tempIntTime":{"$lt":%d}}, initial={"rate": 0}, reduce=reducer)''' %(mongodbdb ,mongodbaudiencerate, self.tempTime))
		self.result = sorted(self.result, key = lambda x:x['rate'], reverse=True)

	def insertChannelTOPN(self):
		#insert
		for doc in self.result:
			# no index left
			if len(self.insertLeft)==0:
				break
			if doc['channelId'] in self.insertList:
				continue
			self.insertList.append(doc['channelId'])




			tempRank = min(self.insertLeft)
			
			pycursor = eval('''self.pyconn.%s.%s.find({'channelId':'%s'}).limit(1).batch_size(30)''' %(mongodbdb ,mongodbaudiencerate, str(doc['channelId'])))

			for channelRate in pycursor:
# 				self.maxpk+=1
				data = list()
# 				data.append(str(self.maxpk))
				data.append(doc['channelId'])
				# data.append(getVSPdata('a').getOriginalIDThroughCode(doc['channelId']))
				data.append(str(channelRate['channelName']))
				data.append('')
				data.append('0')
				data.append(str(tempRank))
				data.append(str(doc['rate']))
				data.append('1')
				data.append('0')
				data.append('1')
				data.append('1')
				data.append('0')
				data.append('0')
				data.append(str(self.createTime))
				self.insertDict[tempRank] = tuple(data)
				self.insertLeft.remove(tempRank)

			pycursor.close()
		self.pyconn.close()

		for _,data in self.insertDict.iteritems():
			self.mysqlcursor.execute("insert into ire_channel_topn(channel_id, channel_name, channel_pic, main_rank, sub_rank, score, systype, status, main_type, sub_type, isMainLock, isSubLock, create_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , tuple(data))
			self.mysqlconn.commit()
			
		# update redis
		ret = refresh().refreshTable('broadcast')
		if ret == 0:
			infologger.error("!!!!!!!!!\n\nREDIS fail to UPDATE\n\n!!!!")
		else:
			infologger.info('Update redis succeed!')


if __name__=='__main__':
	test = processChannelTopN(300)
	test.readLock()
	test.freshPrevious()
	test.calculateRate()
	test.insertChannelTOPN()

	del test






