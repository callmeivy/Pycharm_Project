#coding:UTF-8
'''
Created on 2014.7.25

@author: hao
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# log
import datetime
date = str(datetime.datetime.today()).split(' ')[0]
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename='mfdb_rec_%s.log' %date, filemode='a')
infologger = logging.getLogger()

from ConfigParser import ConfigParser
try:
    config = ConfigParser()
    config.read('/var/IAE/config.ini')
    mysqlhost = config.get("mysql", "ip")
    mysqluser = config.get("mysql", "user")
    mysqlpasswd = config.get("mysql", "passwd")
    mysqldb = config.get("mysql", "database")
    
    mysqlelementinfo = config.get("mfdb", "elementInfo")
    mysqlcataloginfo = config.get("mfdb", "catalogInfo")
    mysqlapplicationinfo = config.get("mfdb", "applicationInfo")
    
    mysqlrealation = config.get('mfdb','relation')
    
except Exception, e:
    infologger.error(e)
    sys.exit(1)

import MySQLdb
class createRootListTable():
    '''
    create 6 layers mysql table
    '''
    def __init__(self):
        self.mysqlconn = MySQLdb.connect(host = mysqlhost, user = mysqluser, passwd = mysqlpasswd, db = mysqldb, charset = 'utf8')
        self.mysqlcursor = self.mysqlconn.cursor()
        
        self.seriesList = list()
        
        # store the end point
        self.tree = dict()
        # store the relation of ids and their parent id and names
        self.relationTree = dict()
        # uniform time
        self.insertTime = datetime.datetime.now()
        # create table if not exists
        self.mysqlcursor.execute('drop table if exists %s;' %mysqlrealation)
        self.mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS %s(
        pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentName VARCHAR(255), contentId bigint(20), level1Name VARCHAR(255), level1Id bigint(20),
        level2Name VARCHAR(255), level2Id bigint(20), level3Name VARCHAR(255), level3Id bigint(20), level4Name VARCHAR(255), level4Id bigint(20),
        level5Name VARCHAR(255), level5Id bigint(20), level6Name VARCHAR(255), level6Id bigint(20), seriesType VARCHAR(10), create_date DATETIME,
        resource_time DATETIME) charset=utf8
        ''' %mysqlrealation)
        # 
#         self.mysqlcursor.execute('select count(*) from %s;' %(mysqlrealation))
#         try:
#             self.pk = self.mysqlcursor.fetchall()[0][0]
#         except:
#             self.pk = 0
        
    def __del__(self):
        self.mysqlcursor.close()
        self.mysqlconn.close()
    
    def processTree(self, subId, rootId, rootName):
        '''
        process every sort ids included in catalog
        '''
        self.relationTree[subId] = dict()
        self.relationTree[subId]['rootId'] = rootId
        self.relationTree[subId]['rootName'] = rootName
        try:
            self.mysqlcursor.execute('select name, sort_index, type from %s where id = %s limit 1;' %(mysqlcataloginfo, subId))
            result = self.mysqlcursor.fetchall()
            if len(result)<=0:
                self.tree[subId] = dict()
                self.tree[subId]['rootId'] = rootId
                self.tree[subId]['rootName'] = rootName
                self.tree[subId]['type'] = '0'
            elif result[0][2]=='1':
                self.tree[subId] = dict()
                self.tree[subId]['rootId'] = rootId
                self.tree[subId]['rootName'] = rootName
                self.tree[subId]['type'] = '1'
            else:
                title = result[0][0]
                if result[0][1] is not None and len(result[0][1])>0:
                    tempIds = result[0][1].strip().split(';')
                    for tempId in tempIds:
                        self.processTree(tempId, subId, title)
        except Exception,e:
            infologger.error(e)
            
    def traversal(self):
        '''
        traverse all the application items
        '''
        self.mysqlcursor.execute('select catalog_id from %s;' %mysqlapplicationinfo)
        catalogIds = self.mysqlcursor.fetchall()
        # for all applications
        for catalogId in catalogIds:
            insertData = list()
            insertCount = 0
            if catalogId[0]==0:
                continue

            self.tree = dict()
            self.relationTree = dict()
            # build tree start from every root node
            self.processTree(str(catalogId[0]).decode('utf8'), 0, '')

            for treeId, parentInfo in self.tree.iteritems():
                # for every leaf node, insert into tree
                tempData = self.insertTree(treeId, parentInfo)
                if tempData is None:
                    continue
                insertData.append(tuple(tempData))
                insertCount+=1
                if insertCount>=10:
                    try:
                        self.mysqlcursor.executemany("insert into ire_content_relation(contentName, contentId, level1Name, level1Id, level2Name, level2Id, level3Name, level3Id, level4Name, level4Id, level5Name, level5Id, level6Name, level6Id, seriesType, create_date, resource_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , insertData)
                        self.mysqlconn.commit()
                    except:
                        print insertData
                    insertCount=0
                    insertData = list()
            if insertCount>0:
                self.mysqlcursor.executemany("insert into ire_content_relation(contentName, contentId, level1Name, level1Id, level2Name, level2Id, level3Name, level3Id, level4Name, level4Id, level5Name, level5Id, level6Name, level6Id, seriesType, create_date, resource_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , insertData)
                self.mysqlconn.commit()
            
    def insertTree(self, leafId, rootInfo):
        '''
        insert mysql
        '''
        tempTreeList = list()
        rootId = rootInfo['rootId']
        rootName = rootInfo['rootName']
        rootType = rootInfo['type']
        if rootId == 0:
            return None
        while True:
            tempTreeList.append((rootId, rootName))
            if self.relationTree[rootId]['rootId']==0:
                break
            rootName = self.relationTree[rootId]['rootName']
            rootId = self.relationTree[rootId]['rootId']
        # [(u'10000175', u'\u79fb\u52a8WLAN Windows\u7cfb\u7edf'), (u'10000169', u'\u98de\u89c6\u64cd\u4f5c\u6307\u5357'), (u'10000168', u'\u6b4c\u534e\u98de\u89c6')]
        count = len(tempTreeList)      
        tempData = list()
        
        self.mysqlcursor.execute('select title, create_time, locking from %s where id = %s limit 1;' %(mysqlelementinfo, leafId))
        tempRes = self.mysqlcursor.fetchall()
        if len(tempRes)==0:
            self.mysqlcursor.execute('select name, create_time, locking from %s where id = %s limit 1;' %(mysqlcataloginfo, leafId))
            tempRes = self.mysqlcursor.fetchall()
        try:
            tempData.append(tempRes[0][0])
        except:
            return None
        # filtering the locking for expired resource
        if tempRes[0][2] == '1' or tempRes[0][2] == '3':
            return None
        tempData.append(leafId)      
        for _ in range(min(count,6)):
            # depth less than 6
            tempParent = tempTreeList.pop()
            tempData.append(tempParent[1])
            tempData.append(tempParent[0])

        for _ in range(6-count):
            tempData.append('')
            tempData.append('0')
        
        tempData.append(rootType)    
        tempData.append(self.insertTime)
        try:
            tempData.append(tempRes[0][1])
        except:
            return None
        return tempData
        
            
if __name__=='__main__':
    test = createRootListTable()
    test.traversal()

    del test
    
    
    
    