# coding=UTF-8
import pymongo


# #2014-09-22 0:0:0 ---2014-09-22 23:59:59
#8-25 1408896000000  1408982399999
#9-4  1409760000000  1409846399999
#9-3  1409673600000  1409759999999
######
###tv
lines = movieInfo_list.find({"updateTimestamp":{"$gt":1409673600000,"$lt":1409759999999},"MovieInfo.TypeID":"2"}).batch_size(30)
tv_list=[]
count=0
for line in lines:
    count+=1
print count
    # if "MovieID" in line["MovieInfo"]:
    #     tv_id=line['MovieInfo']['MovieID']
    #     print tv_id
    # if tv_id not in tv_list:
    #     tv_list.append(tv_id)

print "8.24 tv",len(tv_list)
#movie
lines = movieInfo_list.find({"updateTimestamp":{"$gt":1409673600000,"$lt":1409759999999},"MovieInfo.TypeID":"1"}).batch_size(30)
count=0

for line in lines:
    count+=1

print "8.24 movie",count


#
# #2014-08-25 0:0:0 ---2014-08-25 23:59:59
# lines2 = movieInfo_list.find({"updateTimestamp":{"$gt":1408896000000,"$lt":1408982399999}}).batch_size(30)
# count1=0
# for line in lines2:
#     count1+=1
# print "8.25",count1
#
#
# #2014-08-24 0:0:0 ---2014-08-24 23:59:59
# lines3 = movieInfo_list.find({"updateTimestamp":{"$gt":1408809600000,"$lt":1408895999999}}).batch_size(30)
# count2=0
# for line in lines3:
#     count2+=1
# print "8.24",count2
#
#
# #2014-09-21 0:0:0 ---2014-09-21 23:59:59
# lines4 = movieInfo_list.find({"updateTimestamp":{"$gt":1411228800000,"$lt":1411315199999}}).batch_size(30)
# count3=0
# for line in lines4:
#     count3+=1
# print "9.21",count3
#
#
# #2014-08-23 0:0:0 ---2014-08-23 23:59:59
# lines5 = movieInfo_list.find({"updateTimestamp":{"$gt":1408723200000,"$lt":1408809599999}}).batch_size(30)
# count4=0
# for line in lines5:
#     count4+=1
# print "8.23",count4
#
#
#
# #2014-09-04 0:0:0 ---2014-09-04 23:59:59
# lines6 = movieInfo_list.find({"updateTimestamp":{"$gt":1409760000000,"$lt":1409846399999}}).batch_size(30)
# count5=0
# for line in lines6:
#     count5+=1
# print "9.4",count5


# #2014-09-29 0:0:0 ---2014-09-29 23:59:59
# lines7 = movieInfo_list.find({"updateTimestamp":{"$gt":1409760000000,"$lt":1409846399999}}).batch_size(30)
# count6=0
# for line in lines7:
#     count6+=1
# print "9.29",count6


# #2014-09-25 0:0:0 ---2014-09-25 23:59:59
# lines8 = movieInfo_list.find({"updateTimestamp":{"$gt":1411574400000,"$lt":1411660799999}}).batch_size(30)
# count7=0
# for line in lines8:
#     count7+=1
# print "9.25",count7



# #2014-09-3 0:0:0 ---2014-09-3 23:59:59
# lines9 = movieInfo_list.find({"updateTimestamp":{"$gt":1409673600000,"$lt":1409759999999}}).batch_size(30)
# count8=0
# for line in lines9:
#     count8+=1
# print "9.3",count8

conn.close()