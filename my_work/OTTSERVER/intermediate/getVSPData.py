#coding:UTF-8
'''
Created on 2014.6.26

get one-one relation from VSP data
import class and set like getVSPdata('b').getPAID('A1000260081')

Movie and Huikan share same interface MV
TVseries use the interface TV

@author: hao
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import logging
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a %b %d %H:%M:%S %Y',
                filename='getVSPData.log',
                filemode='a')
infologger = logging.getLogger("getVSPData")

from ConfigParser import ConfigParser
import MySQLdb

class getVSPdata():
    def __init__(self, district):
        try:
            config = ConfigParser()
            config.read('../config.ini')
            self.mysqlhost = config.get("vsp", "ip")
            self.mysqluser = config.get("vsp", "user")
            self.mysqlpasswd = config.get("vsp", "passwd")
            if district == 'a':
                self.mysqldb = config.get("vsp", "BOdatabaseA")
            elif district == 'b':
                self.mysqldb = config.get("vsp", "BOdatabaseB")
            elif district == 'c':
                self.mysqldb = config.get("vsp", "BOdatabaseC")
            elif district == 'd':
                self.mysqldb = config.get("vsp", "BOdatabaseD")
            elif district == 'e':
                self.mysqldb = config.get("vsp", "BOdatabaseE")
            elif district == 'f':
                self.mysqldb = config.get("vsp", "BOdatabaseF")
            elif district == 'g':
                self.mysqldb = config.get("vsp", "BOdatabaseG")
            else:
                raise Exception("no such district")
            self.mysqlelementInfo = config.get("vsp", "elementInfo")
            self.mysqlcatelogInfo = config.get("vsp", "catalogInfo")
            self.mysqlelementExtra = config.get("vsp", "elementExtra")
            self.mysqlcatelogExtra = config.get("vsp", "catalogExtra")
        except Exception, e:
            infologger.error(e)
            sys.exit(1)

    def requireMysqlOneTerm(self, termName, tableName, conditionName, conditionValue):
        '''
        fomat get 'select %s from %s where %s = "%s" limit 1;'
        '''
        mysqlconn = MySQLdb.connect(host = self.mysqlhost, user = self.mysqluser, passwd = self.mysqlpasswd, db = self.mysqldb, charset = 'utf8',use_unicode=False)
        mysqlcursor = mysqlconn.cursor()
        mysqlcursor.execute('select %s from %s where %s = "%s" limit 1;' %(termName, tableName, conditionName, conditionValue))
        try:
            returnValue = mysqlcursor.fetchall()[0][0]
        except:
            returnValue = -1
        mysqlcursor.close()
        mysqlconn.close()
        return returnValue

    def requireMysqlLikeOneTerm(self, termName, tableName, conditionName, conditionValue):
        '''
        fomat get 'select pid from %s where original_id LIKE "%% %(self.mysqlelementInfo) + '%s' %PAID + '%";'
        '''
        mysqlconn = MySQLdb.connect(host = self.mysqlhost, user = self.mysqluser, passwd = self.mysqlpasswd, db = self.mysqldb, charset = 'utf8',use_unicode=False)
        mysqlcursor = mysqlconn.cursor()
        sqlString = 'select %s from %s where %s LIKE "%%' %(termName, tableName, conditionName) + '%s' %conditionValue + '%" limit 1;'
        mysqlcursor.execute(sqlString)
        try:
            returnValue = mysqlcursor.fetchall()[0][0]
        except:
            returnValue = -1
        mysqlcursor.close()
        mysqlconn.close()
        return returnValue


#################################  MV and HK   #################################################    
    def getMVPAID(self, localID):
        '''
        get movie paid through localID
        '''
        mysqlResult = self.requireMysqlOneTerm('original_id', self.mysqlelementInfo, 'extra_1', localID)
        if mysqlResult == -1:
            return -1
        try:
            return mysqlResult.split(';')[1]
        except Exception, e:
            infologger.error(e)
            return -1
    
    def getMVPID(self, localID):
        '''
        get movie pid through localID
        '''
        mysqlResult = self.requireMysqlOneTerm('original_id', self.mysqlelementInfo, 'extra_1', localID)
        if mysqlResult == -1:
            return -1
        try:
            return mysqlResult.split(';')[0]
        except Exception, e:
            infologger.error(e)
            return -1
    
    def getMVNAME(self, localID):        
        '''
        get movie name through localID
        '''
        mysqlResult = self.requireMysqlOneTerm('title', self.mysqlelementInfo, 'extra_1', localID)
        if mysqlResult == -1:
            return ''
        return mysqlResult
        
    def getMVPIC(self, localID):
        '''
        get movie picture through localID
        '''
        mysqlResult = self.requireMysqlOneTerm('poster', self.mysqlelementInfo, 'extra_1', localID)
        if mysqlResult == -1:
            return ''
        return mysqlResult

    def getMVPIDThroughPAID(self, PAID):
        '''
        get tv pid through paid
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('pid', self.mysqlelementInfo, 'original_id', PAID)
        return mysqlResult

    def getMVNAMEThroughPAID(self, PAID):
        '''
        search movie name through paid
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('title', self.mysqlelementInfo, 'original_id', PAID)
        if mysqlResult == -1:
            return ''
        return mysqlResult
###############################################################################################
    
    def getTVPAID(self, localTVID):
        '''
        get tv paid through localTVID
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('paid', self.mysqlcatelogInfo, 'original_id', localTVID)
        return mysqlResult
    
    def getTVPID(self, localTVID):
        '''
        get tv pid through localTVID
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('pid', self.mysqlcatelogInfo, 'original_id', localTVID)
        return mysqlResult

    def getTVNAME(self, localTVID):
        '''
        get tv name through localTVID
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('name', self.mysqlcatelogInfo, 'original_id', localTVID)
        if mysqlResult == -1:
            return ''
        return mysqlResult
    
    def getTVPIC(self, localTVID):
        '''
        get tv picture through localTVID
        '''
        mysqlResult = self.requireMysqlLikeOneTerm('poster', self.mysqlcatelogInfo, 'original_id', localTVID)
        if mysqlResult == -1:
            return ''
        return mysqlResult

###############################################################################################

    def getCHANNELnameThroughOriginalID(self, original_id):
        '''
        get channel name through original id
        '''
        mysqlResult = self.requireMysqlOneTerm('name', self.mysqlcatelogInfo, 'original_id', original_id)
        if mysqlResult==-1:
            return ''
        return mysqlResult

    def getCHANNELnameThroughCode(self, code):
        '''
        get channel name through original id
        '''
        info_id = self.requireMysqlOneTerm('info_id', self.mysqlcatelogExtra, 'extra_29', code)
        if info_id == -1:
            return ''
        mysqlResult = self.requireMysqlOneTerm('name', self.mysqlcatelogInfo, 'id', info_id)
        if mysqlResult == -1:
            return ''
        return mysqlResult

    def getOriginalIDThroughCode(self, code):
        '''
        get channel name through original id
        '''
        info_id = self.requireMysqlOneTerm('info_id', self.mysqlcatelogExtra, 'extra_29', code)
        if info_id == -1:
            return -1
        mysqlResult = self.requireMysqlOneTerm('original_id', self.mysqlcatelogInfo, 'id', info_id)
        if mysqlResult == -1:
            return -1
        return mysqlResult

    def getCodeThroughOriginalID(self, original_id):
        '''
        get channel name through original id
        '''        
        tempid = self.requireMysqlOneTerm('id', self.mysqlcatelogInfo, 'original_id', original_id)
        if tempid == -1:
            return -1
        mysqlResult = self.requireMysqlOneTerm('extra_29', self.mysqlcatelogExtra, 'info_id', tempid)
        if mysqlResult == -1:
            return -1
        return mysqlResult

if __name__=='__main__':
#     print getVSPdata('a').getMVPAID('A1000773684')
#     print getVSPdata('a').getMVPID('A1000773684')
#     print getVSPdata('a').getMVNAME('A1000773684')
# 	print getVSPdata('a').getMVPIC('A1000773684')
    print getVSPdata('a').getMVPIDThroughPAID('GEHU6051407060630000')
#     print getVSPdata('a').getMVNAMEThroughPAID('SITV2010000004495714')

#     print getVSPdata('a').getTVPAID('longmenbiaoju')
#     print getVSPdata('a').getTVPID('longmenbiaoju')
#     print getVSPdata('a').getTVNAME('longmenbiaoju')
#     print getVSPdata('a').getTVPIC('longmenbiaoju')


#     print getVSPdata('a').getCHANNELnameThroughOriginalID('23536')
#     print getVSPdata('a').getOriginalIDThroughCode('621')
#     print getVSPdata('a').getCHANNELnameThroughCode('621')
#     print getVSPdata('a').getCodeThroughOriginalID('23536')




