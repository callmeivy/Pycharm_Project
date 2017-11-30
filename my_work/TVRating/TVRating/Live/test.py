# coding=UTF-8
import MySQLdb
import datetime
now = datetime.datetime.now()
print now
date = (datetime.datetime.now() - datetime.timedelta(days = 39))
date = str(date).split(' ')[0]
print date

# for i in range(0,1000000):
#     print i
# now2 = datetime.datetime.now()
# print now2
# interval = now2 - now
# print "共耗时：",interval
#
#
# sample_number = float(50000)/float(1.98)
# print sample_number
# mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
# mysqlcursor = mysqlconn.cursor()
# mysqlcursor.execute('select * from audienceRate_min limit 1;')
# tmpType = mysqlcursor.fetchall()
# for one in tmpType:
#     print one
#
#
# mysqlcursor.close()
# mysqlconn.close()
# count = 59
# count = count - 1
# if count > 60:
#     # yu is min,chu_shu is hour
#     yu = count % 60
#     print yu
#     chu_shu = count / 60
#     print chu_shu
#     if len(str(yu)) < 2:
#         yu = '0'+str(yu)
#     else:
#         yu = str(yu)
#     if len(str(chu_shu)) < 2:
#         chu_shu = '0'+str(chu_shu)
#     else:
#         chu_shu = str(chu_shu)
#     period = chu_shu+yu
# else:
#     period = '00'+str(count)
#
# print period
# print 61%60
# print 168/60

# import pymongo
# import fractions
# import numpy as np
# import MySQLdb
# import datetime
# date = (datetime.datetime.now() - datetime.timedelta(days = 165))
# date = str(date).split(' ')[0]
# conn=pymongo.Connection('172.16.168.45',27017)
# iae_audiencerate_new=conn.gehua.iae_audiencerate_new
# lines=eval("iae_audiencerate_new.find({'WIC.date':'%s'}).batch_size(30)" %(date))
# channel_box = list()
# for line in lines:
#     L=len(line['WIC']['A'])
#     if type(line['WIC']['A']) is list:
#         for s in range(0,L):
#             # if line['WIC']['A'][s]['p']=='%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F':
#             channel = line['WIC']['A'][s]['sn'].encode('utf-8')
#             if channel not in channel_box:
#                 channel_box.append(channel)
#     else:
#         channel = line['WIC']['A']['sn'].encode('utf-8')
#         if channel not in channel_box:
#                 channel_box.append(channel)
# print channel_box

# conn=pymongo.Connection('172.16.168.45',27017)


# 将所有频道抓出来！！！！！！！！！！！！！！！！！！！！！！！！！！！！
# conn=pymongo.Connection('10.3.2.63',27017)
# iae_audiencerate_new=conn.gehua.iae_audiencerate_new
# lines=eval("iae_audiencerate_new.find({'WIC.date':'%s'}).batch_size(30)" %(date))
# channel_box = list()
# for line in lines:
#     try:
#         L=len(line['WIC']['A'])
#         if type(line['WIC']['A']) is list:
#                 for s in range(0,1):
#                     channel = line['WIC']['A'][s]['n']
#                     if channel not in channel_box:
#                         channel_box.append(channel)
#         else:
#             channel = line['WIC']['A']['n']
#             if channel not in channel_box:
#                 channel_box.append(channel)
#     except:
#         L = 0
#         channel = ""
# print channel_box
#
# for one in channel_box:
#     print one
#
# conn.close()

# 将频道名称对应出来
# channel = ['时代出行','天津卫视高清','金色频道','CCTV6电影高清','浙江卫视高清','游戏风云','中华美食','CCTV8电视剧高清','江苏卫视','BTV新闻','CCTV-15 音乐','游戏竞技','四川卫视','炫动卡通','安徽卫视','CCTV-4 中文国际','电子体育','茶频道','天元围棋','弈坛春秋','时代家居','山东卫视高清','动漫秀场','央广购物','CCTV-10 科教','CHC家庭影院','CCTV-11 戏曲','风云音乐','CCTV-12 社会与法','世界地理','空中课堂','时代风尚','BTV体育','怀旧剧场','BTV科教','CCTV-13 新闻','兵团卫视','女性时尚','风云剧场','河南卫视','金鹰卡通','篮球','CHC动作电影','辽宁卫视高清','全纪实','湖北卫视','家庭理财','风尚购物','劲爆体育','江西卫视','东方购物','第一剧场','CCTV1综合高清','黑龙江卫视高清','BTV文艺','国防军事','广东卫视','优漫卡通','东方卫视','环球奇观','央视文化精品','四海钓鱼','新科动漫','置业频道','CHC高清电影','靓妆','电视指南','CCTV-14 少儿','新疆卫视','云南卫视','宁夏卫视','BTV生活','天津卫视','京视剧场','早期教育','BTV北京卫视高清','西藏卫视','BTV体育高清','嘉佳卡通','财富天下','家家购物','青海卫视','中国气象频道','安徽卫视高清','爵士音乐','法治天地','山西卫视','新娱乐','吉林卫视','浙江卫视','风云足球','深圳卫视高清','湖北卫视高清','书画频道','快乐宠物','BTV青年','广东卫视高清','山东卫视','辽宁卫视','福建东南卫视','高尔夫.网球','BTV文艺高清','深圳卫视','都市剧场','百姓健康','BTV影视','CCTV-NEWS','东方卫视高清','广西卫视','黑龙江卫视','时代美食','旅游卫视','生活时尚','BTV KAKU少儿','欢笑剧场','陕西卫视','东方财经','江苏卫视高清','内蒙古卫视','重庆卫视高清','环球购物','车迷频道','BTV北京卫视','CCTV-7 军事农业','湖南卫视高清','CCTV-6 电影','甘肃卫视','发现之旅','CCTV-9 纪录','CCTV-8 电视剧','CCTV-3 综艺','BTV财经','CCTV-2 财经','七彩戏剧','CCTV-5 体育','家有购物','贵州卫视','厦门卫视','CCTV3综艺高清','CCTV-1 综合','现代女性','优优宝贝','CCTV5体育高清','CCTV5+体育赛事','河北卫视','快乐购物','极速汽车','环球旅游','重庆卫视','湖南卫视','证券资讯','老故事']
# for one in channel:
#     lines=eval("iae_audiencerate_new.find({'WIC.A.sn':'%s'}).limit(1).batch_size(30)" %(one))
#     for line in lines:
#         L=len(line['WIC']['A'])
#         if type(line['WIC']['A']) is list:
#             continue
#         else:
#             channel_no = line['WIC']['A']['n']
#             print one,channel_no
conn.close()
