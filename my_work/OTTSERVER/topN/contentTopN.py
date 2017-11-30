'''
this code is relating to the record of iae_ottserver_log and iae_uap_movieInfo_list
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
wholeresource=[]
moiveresource=[]
nonetyperesource=[]
tvresource=[]
movieidList=list()
tvidList=list()
nonetypeidlist=list()
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
        resourceCode=line.get('Paramaters').get('resourceCode')
        print resourceCode
        if resourceCode is not None:
            wholeresource.append(resourceCode)
print "wholeresource",wholeresource

for oneresource in wholeresource:
    print "oneresource",oneresource
    # number=wholeresource.count(oneresource)
    # print number
    GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(oneresource)}).limit(1).batch_size(30)
    # GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({"MovieInfo.MovieID":"105234"})
    for OneGetmovieinfoThroughresourceCode in GetmovieinfoThroughresourceCode:
        GetTypeIDThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['TypeID']
        GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['MovieName']
        print GetTypeIDThroughresourceCode
        print GetMovieNameThroughresourceCode
        if GetTypeIDThroughresourceCode=="1":
            moiveresource.append(oneresource)


        elif GetTypeIDThroughresourceCode=="2":
            tvresource.append(oneresource)

        elif GetTypeIDThroughresourceCode is None:
            nonetyperesource.append(oneresource)
print "moiveresource",moiveresource
print "tvresource",tvresource

print "nonetyperesource",nonetyperesource
moiveresource1 = dict(Counter(tuple(moiveresource)))
print dict(Counter(tuple(tvresource)))
print dict(Counter(tuple(nonetyperesource)))




movierank=dict()
movierankleft= range(1,21,1)
tvrank=dict()
tvrankleft= range(1,21,1)
nonetyperank=dict()
nonetyperankleft= range(1,21,1)



'''
insert movie
'''

# for onemovieresource in set(moiveresource):
for onemovieresource, itercount in moiveresource1.iteritems():
    # print "set(moiveresource)",set(moiveresource)
    print "onemovieresource",onemovieresource
    print "count",itercount
    GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(onemovieresource)}).limit(1).batch_size(30)
    # GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({"MovieInfo.MovieID":"105234"})
    for OneGetmovieinfoThroughresourceCode in GetmovieinfoThroughresourceCode:
        GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['MovieName']
        GetcolumnidThroughresourceCode=OneGetmovieinfoThroughresourceCode['ColumnID']
    print "GetcolumnidThroughresourceCode",GetcolumnidThroughresourceCode



    tempInsert = list()
    tempmovie=min(movierankleft)
    print GetMovieNameThroughresourceCode
    if onemovieresource not in movieidList:
        tempInsert.append('')
        tempInsert.append(str(onemovieresource))
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(GetMovieNameThroughresourceCode.encode('utf-8'))

        tempInsert.append('')
        tempInsert.append('')
        #main rank
        tempInsert.append('0')
        #sub rank is the sorting order
        tempInsert.append(str(tempmovie))
        #score
        tempInsert.append(str(itercount))
        #below is sys_type
        tempInsert.append('1')
        #status
        tempInsert.append('0')
        # main type is colum ID
        tempInsert.append(str(GetcolumnidThroughresourceCode))
        #sub type
        tempInsert.append('0')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(str(createTime))
        movierank[tempmovie] = tuple(tempInsert)
        movieidList.append(onemovieresource)
        print movieidList
        movierankleft.remove(tempmovie)

'''
insert tv
'''


for onetvresource in set(tvresource):
    # print "set(moiveresource)",set(moiveresource)
    print "onetvresource",onetvresource
    tempInsert2 = list()
    temptv=min(tvrankleft)
    if onetvresource not in tvidList:
        tempInsert.append('')
        tempInsert.append(str(onetvresource))
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(GetMovieNameThroughresourceCode.encode('utf-8'))

        tempInsert.append('')
        tempInsert.append('')
        #main rank
        tempInsert.append('0')
        #sub rank is the sorting order
        tempInsert.append('')
        #score
        tempInsert.append('')
        #below is sys_type
        tempInsert.append('1')
        #status
        tempInsert.append('0')
        # main type is colum ID
        tempInsert.append(str(GetcolumnidThroughresourceCode))
        #sub type
        tempInsert.append('1')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert2.append(str(createTime))
        tvrank[temptv] = tuple(tempInsert2)
        tvidList.append(onetvresource)
        print tvidList
        tvrankleft.remove(temptv)



'''
insert nonetype resource
'''


for onenonetyperesource in set(nonetyperesource):
    # print "set(moiveresource)",set(moiveresource)
    print "onenonetyperesource",onenonetyperesource
    tempInsert3 = list()
    tempnonetype=min(nonetyperankleft)
    if onenonetyperesource not in nonetypeidlist:
        tempInsert3.append('')
        tempInsert3.append(str(onenonetyperesource))
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append(GetMovieNameThroughresourceCode.encode('utf-8'))
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('NONE')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append(str(createTime))
        nonetyperank[tempnonetype] = tuple(tempInsert3)
        nonetypeidlist.append(onenonetyperesource)
        print nonetypeidlist
        nonetyperankleft.remove(tempnonetype)


'''
subtype is 3
'''
#3


# resourceCode=doc['Paramaters']['resourceCode']
# pycursor = conn.%s.find({"Paramaters.resourceCode":"%s".batch_size(30)' %(iae_ottserver_log, str(resourceCode))


for oneresource1 in wholeresource:
    print "oneresource",oneresource1
    # number=wholeresource.count(oneresource)
    # print number
    GetmovieinfoThroughresourceCode1=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(oneresource)}).limit(1).batch_size(30)
    # GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({"MovieInfo.MovieID":"105234"})
    for OneGetmovieinfoThroughresourceCode in GetmovieinfoThroughresourceCode1:
        GetTypeIDThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['TypeID']
        GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['MovieName']
        print GetTypeIDThroughresourceCode
        print GetMovieNameThroughresourceCode
        if GetTypeIDThroughresourceCode=="1":
            moiveresource.append(oneresource)


        elif GetTypeIDThroughresourceCode=="2":
            tvresource.append(oneresource)

        elif GetTypeIDThroughresourceCode is None:
            nonetyperesource.append(oneresource)
print "moiveresource",moiveresource
print "tvresource",tvresource

print "nonetyperesource",nonetyperesource
moiveresource1 = dict(Counter(tuple(moiveresource)))
print dict(Counter(tuple(tvresource)))
print dict(Counter(tuple(nonetyperesource)))




movierank1=dict()
movierankleft1= range(1,21,1)
tvrank1=dict()
tvrankleft1= range(1,21,1)
nonetyperank1=dict()
nonetyperankleft1= range(1,21,1)
movieidList1=list()
tvidList1=list()
nonetypeidlist1=list()


'''
insert movie
'''

# for onemovieresource in set(moiveresource):
for onemovieresource, itercount in moiveresource1.iteritems():
    # print "set(moiveresource)",set(moiveresource)
    print "onemovieresource",onemovieresource
    print "count",itercount
    GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({'MovieInfo.MovieID':str(onemovieresource)}).limit(1).batch_size(30)
    # GetmovieinfoThroughresourceCode=iae_uap_movieInfo_list.find({"MovieInfo.MovieID":"105234"})
    for OneGetmovieinfoThroughresourceCode in GetmovieinfoThroughresourceCode:
        GetMovieNameThroughresourceCode=OneGetmovieinfoThroughresourceCode['MovieInfo']['MovieName']
        GetcolumnidThroughresourceCode=OneGetmovieinfoThroughresourceCode['ColumnID']
    print "GetcolumnidThroughresourceCode",GetcolumnidThroughresourceCode



    tempInsert = list()
    tempmovie1=min(movierankleft1)
    print GetMovieNameThroughresourceCode
    if onemovieresource not in movieidList1:
        tempInsert.append('')
        tempInsert.append(str(onemovieresource))
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(GetMovieNameThroughresourceCode.encode('utf-8'))

        tempInsert.append('')
        tempInsert.append('')
        #main rank
        tempInsert.append('0')
        #sub rank is the sorting order
        tempInsert.append(str(tempmovie1))
        #score
        tempInsert.append(str(itercount))
        #below is sys_type
        tempInsert.append('3')
        #status
        tempInsert.append('0')
        # main type is colum ID
        tempInsert.append(str(GetcolumnidThroughresourceCode))
        #sub type
        tempInsert.append('0')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(str(createTime))
        movierank1[tempmovie1] = tuple(tempInsert)
        movieidList1.append(onemovieresource)
        print movieidList1
        movierankleft1.remove(tempmovie1)

'''
insert tv
'''


for onetvresource in set(tvresource):
    # print "set(moiveresource)",set(moiveresource)
    print "onetvresource",onetvresource
    tempInsert2 = list()
    temptv=min(tvrankleft1)
    if onetvresource not in tvidList:
        tempInsert.append('')
        tempInsert.append(str(onetvresource))
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append(GetMovieNameThroughresourceCode.encode('utf-8'))

        tempInsert.append('')
        tempInsert.append('')
        #main rank
        tempInsert.append('0')
        #sub rank is the sorting order
        tempInsert.append('')
        #score
        tempInsert.append('')
        #below is sys_type
        tempInsert.append('1')
        #status
        tempInsert.append('0')
        # main type is colum ID
        tempInsert.append(str(GetcolumnidThroughresourceCode))
        #sub type
        tempInsert.append('3')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert.append('')
        tempInsert2.append(str(createTime))
        tvrank1[temptv] = tuple(tempInsert2)
        tvidList.append(onetvresource)
        print tvidList
        tvrankleft1.remove(temptv)



'''
insert nonetype resource
'''


for onenonetyperesource in set(nonetyperesource):
    # print "set(moiveresource)",set(moiveresource)
    print "onenonetyperesource",onenonetyperesource
    tempInsert3 = list()
    tempnonetype=min(nonetyperankleft)
    if onenonetyperesource not in nonetypeidlist:
        tempInsert3.append('')
        tempInsert3.append(str(onenonetyperesource))
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append(GetMovieNameThroughresourceCode.encode('utf-8'))
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('NONE')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append('')
        tempInsert3.append(str(createTime))
        nonetyperank1[tempnonetype] = tuple(tempInsert3)
        nonetypeidlist.append(onenonetyperesource)
        print nonetypeidlist
        nonetyperankleft.remove(tempnonetype)










"""
write the results above into mysql
"""
mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ott_content_topn(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentid VARCHAR(255), assetId VARCHAR(255), pid VARCHAR(255), contentname VARCHAR(255),
    channelname VARCHAR(255), contentpic VARCHAR(255), main_rank int(11), sub_rank int(11), score bigint, systype int(11),
    status int(11), main_type int(11), sub_type int(11), OSversion double(4,2), isMainLock int(11), isSubLock int(11), create_date DATETIME) charset=utf8
    ''')




for _, data in movierank.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()
for _, data in tvrank.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()
for _, data in nonetyperank.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()










# """
# write the results above into mysql
# """
# mysqlconn = MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS ott_content_topn(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, contentid VARCHAR(255), assetId VARCHAR(255), pid VARCHAR(255), contentname VARCHAR(255),
#     channelname VARCHAR(255), contentpic VARCHAR(255), main_rank int(11), sub_rank int(11), score bigint, systype int(11),
#     status int(11), main_type int(11), sub_type int(11), OSversion double(4,2), isMainLock int(11), isSubLock int(11), create_date DATETIME) charset=utf8
#     ''')




for _, data in movierank1.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()
for _, data in tvrank1.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()
for _, data in nonetyperank1.iteritems():
    mysqlcursor.execute("insert into ott_content_topn(pk,contentid, assetId, pid, contentname, channelname, contentpic, main_rank, sub_rank, score, systype, status, main_type, sub_type, OSversion, isMainLock, isSubLock, create_date) values (%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , data)
    mysqlconn.commit()






mysqlconn.close()
conn.close()

