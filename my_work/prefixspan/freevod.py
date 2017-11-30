# coding=UTF-8
import pymongo
import MySQLdb
import datetime
import numpy as np
import collections
import sys
from collections import defaultdict

conn=pymongo.Connection('172.16.168.45',27017)
print "connected"
iae_vsp_log=conn.gehua.iae_vsp_log
# index_1=iae_vsp_log.ensure_index("Paramaters.caid")
# caids = iae_vsp_log.find({"date":{"$gt":1418054400000,"$lt":1418140799999}}).distinct('Paramaters.caid')
caids=['1372947635','1372162753']
# print "caids done."
# # print len(caids)
#
mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
print "connect mysql"
mysqlcursor.execute('''drop table if EXISTS freevod_path;''')
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS freevod_path(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, caid VARCHAR(10), path TEXT, create_date DATETIME) charset=utf8
    ''' )
createTime = datetime.datetime.now()
for onecaid in caids:
    id_holder=list()
    lines=iae_vsp_log.find({'$and':[{'Paramaters.caid':onecaid},{"date":{"$gt":1418054400000,"$lt":1418140799999}},{"Method":{"$ne":'vsp_outlet/favorite/list/json'}}]}).sort('date',pymongo.ASCENDING)
    for line in lines:
        id=line.get('Paramaters').get('id')
        if id is None:
            id='None'
        Method=(line.get('Method').encode('ascii'))
        Method_part=Method[-9:]
        if Method_part=="load/page":
            Method_part='111'
        if Method_part=="list/json":
            Method_part='222'
        if Method_part=="tail/json":
            Method_part='333'
        id=str(id)+Method_part
        id_holder.append(id)
    id_holder_str=','.join(id_holder)
    tempInsert=list()
    tempInsert.append(str(onecaid))
    tempInsert.append(id_holder_str)
    tempInsert.append(str(createTime))
    mysqlcursor.execute("insert into freevod_path(caid, path, create_date) values (%s, %s, %s)" , tuple(tempInsert))
    mysqlconn.commit()

mysqlcursor.execute("SELECT path from freevod_path limit 3000")

all_path=mysqlcursor.fetchall()
print "select"
zhuan_count=0
tuijian_count=0
diany_count=0
diansj_count=0
kehuan_count=0
num=0
pathWithTwo=dict()
new_dict=dict()
dict_coordinate=dict()
for one_all_path in all_path:
    #######################Notice one_all_path[0]！！！！！！！！###############################
    path2=str(one_all_path[0]).split(',')
    # print "path",path2
    pathWithIndex=dict(enumerate(path2))
    pathWithTwo[num]=pathWithIndex
    # print pathWithTwo
    num+=1
# print "ori",pathWithTwo
    # caid:1372962936   {0: "(u'1000111281 北京青年06（点播）", 1: '10019867'推荐, 2: '10019942'专题, 3: '10017580'动作,
    # 4: '10017589'情感（实为电视剧）, 5: '10017589'情感翻页6, 6: '10017654'北京青年详情页, 7: '10017654'北京青年list,
    # 8: '1000111281 北京青年06（点播）', 9: "1000111282 北京青年07（点播）'", 10: ')'}


    # {0: "(u'1000113277" 妻子的诱惑44（点播） , 1: '10019942'专题 , 2: '10019867' 推荐, 3: '10017580' 动作（实为电影）, 4: '10017589', 5: '10017590', 6: '10017718',
    #  7: '10017718', 8: '1000113280', 9: '1000113281', 10: '1000113282', 11: '1000113283', 12: '1000113282',
    # 13: '10017718', 14: "10017718'", 15: ')'}

    # pathWithTwo {0: {0: "(u'1000113277load/page", 1: '10019942list/json', 2: '10019867list/json', 3: '10017580list/json',
    # 4: '10017589list/json', 5: '10017590list/json', 6: '10017718tail/json', 7: '10017718list/json', 8: '1000113280load/page',
    # 9: '1000113281load/page', 10: '1000113282load/page', 11: '1000113283load/page', 12: '1000113282load/page',
    # 13: '10017718tail/json', 14: "10017718list/json'", 15: ')'}}
# foo = defaultdict( lambda: defaultdict(float) )
N=max(pathWithTwo)+1
M=max(max(x) for x in pathWithTwo.values())+1

fooarray = np.zeros((N, M))
for index_row,path_dict in pathWithTwo.iteritems():
    # print index_row,type(pathWithTwo[index_row])
    for index_colu,path_ele in path_dict.iteritems():

        if path_ele=='' or path_ele=='(' or path_ele==')' or path_ele=='None222' or path_ele=='None333' or path_ele=='None111' or path_ele=='NaN222':
            path_ele=0
        else:
            # print type(path_ele),path_ele
            path_ele=int(path_ele)
        # print "path_ele",path_ele,type(path_ele)
        # print '3',index_row,index_colu,path_ele
        fooarray[index_row, index_colu] = path_ele
        print fooarray.shape
        column_num=fooarray.shape[1]
        # "10019942list/json"
        if path_ele==int("10019867222"):
            tuijian_count+=1
        if path_ele==int("10019942222"):
            zhuan_count+=1
        if path_ele==int("10017580222"):
            diany_count+=1
        if path_ele==int("10017589222"):
            diansj_count+=1
        if path_ele==int("1000181830333"):
            kehuan_count+=1

# print fooarray,fooarray[1][1]
print 'tuijian_count',tuijian_count
print 'zhuan_count',zhuan_count
print 'diany_count',diany_count
print 'diansj_count',diansj_count
print 'kehuan_count',kehuan_count
# 10019942:zhuanti,10019867222,tuijian，10017580222,movie and action,10017589222 tv and motion
# level_1=[10019867222,10019942222,10017580222,10017589222]
level_1=[1000181830333]
# movie 10017579;10017580;10017581;10017582;10017583;10017584;10017585;10017586
level_2_m=[10017579222,10017580222,10017581222,10017582222,10017583222,10017584222,10017585222,10017586222]
# tv 10017589;10017590;10017591;10017592;10017593;10017594;10017595
level_2_t=[10017589222,10017590222,10017591222,10017592222,10017593222,10017594222,10017595222]

for one_level_1 in level_1:

    b=np.nonzero(fooarray==one_level_1)
    # print b,type(b)
    # print b[0],len(b[0])
    # print b[1]
    # print b[0][0],b[1][0]
    next_can=list()
    for i in range(0,len(b[0])):
        # print b[0][i],b[1][i]
        current_x=b[0][i]
        current_y=b[1][i]
        next_x=current_x
        if current_y==column_num-1:
            next_ele='00000'
        else:
            next_y=current_y+1

            next_ele=fooarray[next_x][next_y]
        next_can.append(next_ele)
    c = collections.Counter(next_can)
    print 'result',one_level_1,c


conn.close()
mysqlconn.close()