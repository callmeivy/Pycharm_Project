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
    if ind>1000:
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


mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
# for oneusercode in set(userCodelist):
#     mysqlcursor.execute("""select region_id from user_info where caid=%s""",(oneusercode))
#     region_id=mysqlcursor.fetchone()

for oneusercode in set(userCodelist):
    mysqlcursor.execute("""select region_id from user_info where caid=%s""",(oneusercode))
    GetmovieinfoThroughusercode=iae_ottserver_log.find({'Paramaters.userCode':str(oneusercode)}).limit(1).batch_size(30)
    for OneGetmovieinfoThroughusercode in GetmovieinfoThroughusercode:
        print "OneGetmovieinfoThroughusercode",OneGetmovieinfoThroughusercode
        if 'resourceCode' in OneGetmovieinfoThroughusercode['Paramaters']:
        # if OneGetmovieinfoThroughusercode['Paramaters']['resourceCode'] is not None:
            GetresourceCodeThroughuserCode=OneGetmovieinfoThroughusercode['Paramaters']['resourceCode']










usercoderank=dict()
usercoderankleft= range(1,21,1)
usercodelist=list()

for oneusercode in set(userCodelist):
    print "set(userCodelist)",set(userCodelist)
    print "oneusercode",oneusercode
    mysqlcursor.execute("""select region_id from user_info where caid=%s""",(oneusercode))
    region_id=mysqlcursor.fetchone()



    tempInsert = list()
    tempusercode=min(usercoderankleft)
    GetmovieinfoThroughusercode=iae_ottserver_log.find({'Paramaters.userCode':str(oneusercode)}).limit(1).batch_size(30)
    for OneGetmovieinfoThroughusercode in GetmovieinfoThroughusercode:
        print "OneGetmovieinfoThroughusercode",OneGetmovieinfoThroughusercode
        if 'resourceCode' in OneGetmovieinfoThroughusercode['Paramaters']:
        # if OneGetmovieinfoThroughusercode['Paramaters']['resourceCode'] is not None:
            GetresourceCodeThroughuserCode=OneGetmovieinfoThroughusercode['Paramaters']['resourceCode']
            # GetmovieinfoThroughresourceCode2=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(GetresourceCodeThroughuserCode)}).limit(1).batch_size(30)
            # for OneGetmovieinfoThroughresourceCode2 in GetmovieinfoThroughresourceCode2:
            #     GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode2['MovieInfo']['MovieName']
            #     GetcolumnidThroughresourceCode=OneGetmovieinfoThroughresourceCode2['ColumnID']
            #     print "GetMovieNameThroughresourceCode",GetMovieNameThroughresourceCode

            tempInsert.append('')
            tempInsert.append(str(oneusercode))
            tempInsert.append(str(GetresourceCodeThroughuserCode))
            tempInsert.append(str(region_id))

            tempInsert.append(str(createTime))



            usercoderank[tempusercode] = tuple(tempInsert)
            usercodelist.append(oneusercode)
            print oneusercode
            usercoderankleft.remove(tempusercode)






"""
write the results above into mysql
"""

mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ott_user_collection(id bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, user_id VARCHAR(255), record_id VARCHAR(255), application_id VARCHAR(255),
    time DATETIME) charset=utf8
    ''')



for _, data in usercoderank.iteritems():
    mysqlcursor.execute("insert into ott_user_collection(id, user_id, record_id, application_id, time) values (%s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()





mysqlconn.close()
conn.close()