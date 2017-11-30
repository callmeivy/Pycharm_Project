#coding:UTF-8
'''
Created on 2014.6.20

calculate the audience rate

@author: hao

audienceRate.py是从iae_guehua_rate2抽取相关记录，
写入到inter_audiencerate. 然后channeltopn调用
inter_audiencerate的相关结果插入到
mysql的ire_channel_topn
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('/opt/IAE/intermediate')

# logger
import logging
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='audienceRate.log',
                filemode='a')
infologger = logging.getLogger("audienceRate")

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
except Exception, e:
    infologger.error(e)
    sys.exit(1)

import pymongo
import time
from searchUserinfo import search
import multiprocessing
from getVSPData import getVSPdata

def segmentTime(startTime, endTime, date):
    '''
    process date

    input:
    tartTime=23:59:51    endTime=00:04:51    date=2014-06-01
    output:
    [('23:59:00', '2014-05-31', 1401551940), ('00:00:00', '2014-06-01', 1401552000)]
    '''
    segmentTimeList = list()
    
    stringTime = time.strptime(startTime+' '+date, '%H:%M:%S %Y-%m-%d')
    startintTime = int(time.mktime(stringTime))
    stringTime = time.strptime(endTime+' '+date, '%H:%M:%S %Y-%m-%d')
    endintTime = int(time.mktime(stringTime))
    if startintTime >= endintTime:
        startintTime -= 86400
    startintTime -= startintTime%60
    endintTime += (59-(endintTime-1)%60)
    
    minutesCount = (endintTime-startintTime)/60
    for i in range(minutesCount):        
        dateTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startintTime+i*60))
        dateTime = dateTime.split(' ')
        segmentTimeList.append((dateTime[1], dateTime[0], startintTime+i*60)) 
    return segmentTimeList

# insert
def insertOneChannel(channelID, logs):
    '''
    insert every channel
    '''
    pyconn = pymongo.Connection(host=mongodbhost, port=27017)
    channelName = str()
    vspdata = getVSPdata('a')
    channelId = vspdata.getOriginalIDThroughCode(channelID)

    insertDict = dict()

    for log in logs:
#         log: {'date': u'2014-06-01', 'channelName': u'CCTV-15 \u97f3\u4e50', 'caid': u'172470173', 'startTime': u'23:57:48', 'endTime': u'00:02:48'}
        timeList = segmentTime(startTime = log['startTime'], endTime=log['endTime'], date=log['date'])
        channelName = log['channelName']
        userInfo = search().getInfo(str(log['caid']))
#         userInfo: {u'updateTime': u'Fri Jun 20 16:22:04 2014', u'userState': u'\u6b63\u5e38', u'addresscommunitty': u'\u5de6\u5bb6\u5e84\u5317\u91cc\u75321', 
#         u'addressdistrict': u'\u671d\u9633\u533a', u'ip': u'', u'userid': u'5968031', u'addressid': u'100004789493', u'stbid': u'', u'userCredit': u'', 
#         u'caid': u'169708143', u'bosscaid': u'11116970814359', u'userOrderInfo': u'', u'_id': ObjectId('53a3ef2cfda35200cf91af68'), u'userTag': u''}
        if len(userInfo)<=0: 
            continue
        for (tempTime, tempDate, tempIntTime) in timeList:
            if (tempTime, tempDate, tempIntTime) in insertDict:
                # avoid record twice
                if str(log['caid']) in insertDict[(tempTime, tempDate, tempIntTime)]['userids']:
                    continue
                insertDict[(tempTime, tempDate, tempIntTime)]['userids'].append(str(log['caid']))
                if len(userInfo['addressdistrict'])>0:
                    if str(userInfo['addressdistrict']) in insertDict[(tempTime, tempDate, tempIntTime)]['userDistrict']:
                        insertDict[(tempTime, tempDate, tempIntTime)]['userDistrict'][str(userInfo['addressdistrict'])] += 1
                    else:
                        insertDict[(tempTime, tempDate, tempIntTime)]['userDistrict'][str(userInfo['addressdistrict'])] = 1
                if len(userInfo['userTag'])>0:
                    if str(userInfo['userTag']) in insertDict[(tempTime, tempDate, tempIntTime)]['userTag']:
                        insertDict[(tempTime, tempDate, tempIntTime)]['userTag'][str(userInfo['userTag'])] += 1
                    else:
                        insertDict[(tempTime, tempDate, tempIntTime)]['userTag'][str(userInfo['userTag'])] = 1
                insertDict[(tempTime, tempDate, tempIntTime)]['rate'] += 1
            
            else:
                tempdict = dict()
                userDistrict = dict()
                userTag = dict()
#                 userids = list()
                tempdict['date'] = tempDate
                tempdict['time'] = tempTime
                tempdict['tempIntTime'] = tempIntTime*1000
                tempdict['channelName'] = channelName
                tempdict['channelId'] = channelId
                tempdict['userids'] = [str(log['caid'])]
                if len(userInfo['addressdistrict'])>0:
                    userDistrict[str(userInfo['addressdistrict'])] = 1
                tempdict['userDistrict'] = userDistrict
                if len(userInfo['userTag'])>0:
                    userTag[str(userInfo['userTag'])] = 1
                tempdict['userTag'] = userTag
                tempdict['rate'] = 1
                insertDict[(tempTime, tempDate, tempIntTime)] = tempdict

    
    for insertdata in insertDict.itervalues():
        pymongoCmd = '''pyconn.%s.%s.insert(%s)''' %(mongodbdb ,mongodbaudiencerate, str(insertdata))
        try:
            exec pymongoCmd
        except Exception,e:
            infologger.error(e)
    pyconn.close()
#是否直接运行该.py文件
if __name__=='__main__':
    pyconn = pymongo.Connection(host=mongodbhost, port=27017)
    record = time.time()
    # 5 minutes = 300 seconds = 300000 milliseconds
    pymongoCmd = "pycursor = pyconn.%s.%s.find({'insertTime':{'$lte':%d}}).batch_size(30)" %(mongodbdb ,mongodbgehuarate, int(time.time()*1000)-300000)

    try:
        exec pymongoCmd
    except Exception,e:
        infologger.error(e)
        sys.exit(1)

    # store every log information
    logDict = dict()

    for doc in pycursor:
        #pycursor:mongo-gehua_rate2 查找的对应channelid的记录
        caid = doc['WIC']['cardNum']
        date = doc['WIC']['date']
        for anchor in doc['WIC']['a']:
            tempDict = dict()
            tempDict['date'] = date
            tempDict['caid'] = caid
            tempDict['startTime'] = anchor['s']
            tempDict['endTime'] = anchor['e']
            tempDict['channelName'] = anchor['sn']
            if str(anchor['n']) not in logDict:
                logDict[str(anchor['n'])] = list()
            logDict[str(anchor['n'])].append(tempDict)

    pycursor.close()
    pyconn.close()
    
    for channelID, logs in logDict.iteritems():
        print channelID
        p = multiprocessing.Process(target=insertOneChannel, args=(channelID, logs,))
        p.start()
    #     p.join()


    
    
    
