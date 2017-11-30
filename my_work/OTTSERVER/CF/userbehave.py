'''
this code is relating to the record of
'''


# coding=UTF-8
#encoding=UTF-8
import pymongo
from collections import Counter
import MySQLdb
import datetime
# conn=pymongo.Connection('10.3.3.220',27017)
conn=pymongo.Connection('172.16.168.45',27017)
iae_ottserver_log=conn.gehua.iae_ottserver_log
iae_uap_movieInfo_list=conn.gehua.iae_uap_movieInfo_list
index = iae_ottserver_log.create_index("_id")
ids=iae_ottserver_log.distinct("_id")
ind=0
userCodelist=[]
createTime = datetime.datetime.now()
for oneid in ids:
    # print oneid

    print ind
    if ind>400:
         break
    ind=ind+1

# resourceCode=doc['Paramaters']['resourceCode']
# pycursor = conn.%s.find({"Paramaters.resourceCode":"%s".batch_size(30)' %(iae_ottserver_log, str(resourceCode))
    lines=iae_ottserver_log.find({"_id":oneid})

    for line in lines:
        userCode=line.get('Paramaters').get('userCode')
        print "userCode",userCode
        if userCode is not None :
            userCodelist.append(userCode)
print "userCodelist",userCodelist


for oneuserCode in userCodelist:
    print "oneuserCode",oneuserCode
    # number=wholeresource.count(oneresource)
    # print number
    GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(oneuserCode)}).limit(1).batch_size(30)
    # GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({"MovieInfo.MovieID":"105234"})
    for OneGetmovieinfoThroughresourceCode in GetmovieinfoThroughresourceCode:
        GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['MovieName']
        print "GetMovieNameThroughresourceCode",GetMovieNameThroughresourceCode













usercoderank=dict()
usercoderankleft= range(1,21,1)
usercodelist=list()

for oneusercode in set(userCodelist):
    print "set(userCodelist)",set(userCodelist)
    print "oneusercode",oneusercode
    tempInsert = list()
    tempusercode=min(usercoderankleft)
    if oneusercode not in usercodelist:
        tempInsert.append('')
        tempInsert.append('')

        tempInsert.append('')
        tempInsert.append(str(oneusercode))
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(str(createTime))
        usercoderank[tempusercode] = tuple(tempInsert)
        usercodelist.append(oneusercode)
        print oneusercode
        usercoderankleft.remove(tempusercode)






"""
write the results above into mysql
"""
mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ott_user_behav(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, userid VARCHAR(255), rec_id VARCHAR(255), rec_assetId VARCHAR(255), rec_name VARCHAR(255),
    rec_channelname VARCHAR(255), rec_pid VARCHAR(255), rec_pic VARCHAR(255), score bigint, reason VARCHAR(255), systype int(11),
     OSversion double(4,2), main_type varchar(100), create_time DATETIME) charset=utf8
    ''')



for _, data in usercoderank.iteritems():
    mysqlcursor.execute("insert into ott_user_prop(pk,addressdistrict, rec_id, rec_assetId, rec_name, rec_channelname, rec_pid, rec_pic, score, systype, status, TV_type, OSversion, isLock, main_type, sub_type, create_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()





mysqlconn.close()
conn.close()
