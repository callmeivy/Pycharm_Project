#coding:UTF-8
'''
Created on 2014年7月25日

@author: hao
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import urllib2
# sys.path.append('/opt/IAE/intermediate')
# logger
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename='refresh.log', filemode='a')
infologger = logging.getLogger()
import simplejson as sj
class refresh():
    '''
    post a request to refresh interface
    '''
    def __init__(self, reqURL1 = 'http://172.16.168.65:8080/DATASERVICE/ire/interfaces/updateredis', reqURL2 = 'http://172.16.168.67:8080/DATASERVICE/ire/interfaces/updateredis'):
        self.reqURL1 = reqURL1
        self.reqURL2 = reqURL2
        self.reqData = dict()
        
    def refreshTable(self, aTableName):
        self.reqData['table'] = aTableName
        self.reqData = sj.dumps(self.reqData)
        req1 = urllib2.Request(url = self.reqURL1,data =self.reqData)
        req2 = urllib2.Request(url = self.reqURL2,data =self.reqData)
        
        try:
            res_data1 = urllib2.urlopen(req1, timeout = 5)
            res1 = res_data1.read()
            res_data2 = urllib2.urlopen(req2, timeout = 5)
            res2 = res_data2.read()
            if eval(res1)['flag']=='sucess' and eval(res2)['flag']=='sucess':
                return 1
            else:
                infologger.error('API return false;')
                return 0
        except:
            infologger.error('Server timeout!')
            sys.exit(1)


if __name__=='__main__':
    refresh().refreshTable('s')

