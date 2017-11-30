# coding=UTF-8
'''
Created on 23 Dec 2014
此代码抓取收视率表中的每一个收视时段，同时将一天中的时间切分到分的层面，对应建立一个1440维的向量，
并将每一个收视时段对应到向量上，出现一整分钟的就加1，不足1分钟且小于31秒的舍弃，大于31秒的转为分数
共耗时： 3:29:15.908000
共耗时： 0:39:12.503000
@author: Jin
'''
import pymongo
import fractions
import numpy as np
import MySQLdb
import datetime

start = datetime.datetime.now()

# conn=pymongo.Connection('172.16.168.45',27017)
conn=pymongo.Connection('10.3.2.63',27017)
iae_audiencerate_new=conn.gehua.iae_audiencerate_new
# index = iae_audiencerate_new.ensure_index("WIC.date")
# index2 = iae_audiencerate_new.ensure_index("WIC.A.sn")
# array = [0] * 2


# ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！某节目的收视率！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
# lines=eval("iae_audiencerate_new.find({'WIC.A.p':'%s','WIC.date':'%s','WIC.A.sn':'%s'}).batch_size(30)" %('%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F','2014-12-05','浙江卫视'))
# 一个频道一天的分钟收视率耗时1.5分钟
# date 为12.5，距离5.20共165天
# days比普通的-1
date = (datetime.datetime.now() - datetime.timedelta(days = 9))
date = str(date).split(' ')[0]
print date
# channel = ['浙江卫视','东方卫视']
# channel = ['浙江卫视','浙江卫视高清']
# channel = ['东方卫视']
# channel = ['CCTV-1 综合']
channel = ['浙江卫视']
# channel = ['CCTV-4 中文国际']
# channel = ['浙江卫视高清','游戏风云','中华美食','CCTV8电视剧高\
# 清','江苏卫视','BTV新闻','游戏竞技','四川卫视','炫动卡通','安徽卫视','电子\
# 体育','茶频道','天元围棋','弈坛春秋','时代家居','山东卫视高清','动漫秀场','央广购物','CHC家\
# 庭影院','风云音乐','世界地理','空中课堂','时代风尚','BTV体育','怀旧剧\
# 场','BTV科教','兵团卫视','女性时尚','风云剧场','河南卫视','金鹰卡通','篮球','CHC动作电影','\
# 辽宁卫视高清','全纪实','湖北卫视','家庭理财','风尚购物','劲爆体育','江西卫视','东方购物','第一剧场','CCTV1\
# 综合高清','黑龙江卫视高清','BTV文艺','国防军事','广东卫视','优漫卡通','东方卫视','环球奇观','央视文化精\
# 品','四海钓鱼','新科动漫','置业频道','CHC高清电影','靓妆','电视指南','新疆卫视','云南卫视','\
# 宁夏卫视','BTV生活','天津卫视','京视剧场','早期教育','BTV北京卫视高清','西藏卫视','BTV体育高清','嘉佳卡\
# 通','财富天下','家家购物','青海卫视','中国气象频道','安徽卫视高清','爵士音乐','法治天地','山西卫视','新娱乐\
# ','吉林卫视','风云足球','深圳卫视高清','湖北卫视高清','书画频道','快乐宠物','BTV青年','广东卫视\
# 高清','山东卫视','辽宁卫视','福建东南卫视','高尔夫.网球','BTV文艺高清','深圳卫视','都市剧场','百姓健\
# 康','BTV影视','CCTV-NEWS','东方卫视高清','广西卫视','黑龙江卫视','时代美食','旅游卫视','生活时尚',\
# '欢笑剧场','陕西卫视','东方财经','江苏卫视高清','内蒙古卫视','重庆卫视高清','环球购物','车迷频\
# 道','BTV北京卫视','湖南卫视高清','甘肃卫视','发现之旅',\
# 'BTV财经','七彩戏剧','家有购物','贵州卫视','\
# 厦门卫视','CCTV3综艺高清','现代女性','优优宝贝','CCTV5体育高清','CCTV5+体育赛事','河北卫视','\
# 快乐购物','极速汽车','环球旅游','重庆卫视','湖南卫视','证券资讯','老故事']
# 以下有空格
# channel = ['CCTV-15 音乐','CCTV-4 中文国际','CCTV-10 科教','CCTV-11 戏曲','CCTV-12 社会与法','CCTV-13 新闻',\
# 'CCTV-7 军事农业','CCTV-6 电影','CCTV-9 纪录','CCTV-8 电视剧','CCTV-3 综艺','CCTV-2 财经','CCTV-5 体育',\
# 'CCTV-1 综合','BTV KAKU少儿','CCTV-14 少儿']
# 完整channel:
# channel = ['时代出行','天津卫视高清','金色频道','CCTV6电影高清','浙江卫视高清','游戏风云','中华美食','CCTV8电视剧高清','江苏卫视','BTV新闻','CCTV-15 音乐','游戏竞技','四川卫视','炫动卡通','安徽卫视','CCTV-4 中文国际','电子体育','茶频道','天元围棋','弈坛春秋','时代家居','山东卫视高清','动漫秀场','央广购物','CCTV-10 科教','CHC家庭影院','CCTV-11 戏曲','风云音乐','CCTV-12 社会与法','世界地理','空中课堂','时代风尚','BTV体育','怀旧剧场','BTV科教','CCTV-13 新闻','兵团卫视','女性时尚','风云剧场','河南卫视','金鹰卡通','篮球','CHC动作电影','辽宁卫视高清','全纪实','湖北卫视','家庭理财','风尚购物','劲爆体育','江西卫视','东方购物','第一剧场','CCTV1综合高清','黑龙江卫视高清','BTV文艺','国防军事','广东卫视','优漫卡通','东方卫视','环球奇观','央视文化精品','四海钓鱼','新科动漫','置业频道','CHC高清电影','靓妆','电视指南','CCTV-14 少儿','新疆卫视','云南卫视','宁夏卫视','BTV生活','天津卫视','京视剧场','早期教育','BTV北京卫视高清','西藏卫视','BTV体育高清','嘉佳卡通','财富天下','家家购物','青海卫视','中国气象频道','安徽卫视高清','爵士音乐','法治天地','山西卫视','新娱乐','吉林卫视','浙江卫视','风云足球','深圳卫视高清','湖北卫视高清','书画频道','快乐宠物','BTV青年','广东卫视高清','山东卫视','辽宁卫视','福建东南卫视','高尔夫.网球','BTV文艺高清','深圳卫视','都市剧场','百姓健康','BTV影视','CCTV-NEWS','东方卫视高清','广西卫视','黑龙江卫视','时代美食','旅游卫视','生活时尚','BTV KAKU少儿','欢笑剧场','陕西卫视','东方财经','江苏卫视高清','内蒙古卫视','重庆卫视高清','环球购物','车迷频道','BTV北京卫视','CCTV-7 军事农业','湖南卫视高清','CCTV-6 电影','甘肃卫视','发现之旅','CCTV-9 纪录','CCTV-8 电视剧','CCTV-3 综艺','BTV财经','CCTV-2 财经','七彩戏剧','CCTV-5 体育','家有购物','贵州卫视','厦门卫视','CCTV3综艺高清','CCTV-1 综合','现代女性','优优宝贝','CCTV5体育高清','CCTV5+体育赛事','河北卫视','快乐购物','极速汽车','环球旅游','重庆卫视','湖南卫视','证券资讯','老故事']

# mysqlconn = MySQLdb.connect(host="172.16.169.12",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlconn = MySQLdb.connect(host="10.3.3.182",user="ire",passwd="ZAQ!XSW@CDE#",db="ire", charset='utf8')
mysqlcursor = mysqlconn.cursor()
mysqlcursor.execute('''CREATE TABLE IF NOT EXISTS audienceRate_min_teeee(
    pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, date_time VARCHAR(10), minute_time VARCHAR(10), channel VARCHAR(20), tvRating VARCHAR(20)) charset=utf8
    ''' )
channel_no = 0
count1 = 0
for one_channel in channel:
    channel_no += 1
    array = [0] * 1440
    # N = eval("iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'%s'}).count().batch_size(30)" %(date,one_channel))
    # print N
    # lines=eval("iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'%s'}).batch_size(30)" %(date,one_channel))
    lines=eval("iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'%s'}).limit(10).batch_size(30)" %(date,one_channel))
    # print "iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'/%s/'}).batch_size(30)" %(date,one_channel)
    # lines=eval("iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'/%s/'}).batch_size(30)" %(date,one_channel))
    try:
        for line in lines:

            # print line
            L=len(line['WIC']['A'])
            if type(line['WIC']['A']) is list:
                for s in range(0,L):
                    # if line['WIC']['A'][s]['p']=='%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F':
                    if line['WIC']['A'][s]['sn'].encode('utf-8')== one_channel:
                        starttime=line['WIC']['A'][s]['s']
                        s_hour=int(starttime[0:2])
                        s_min=int(starttime[3:5])
                        s_sec=int(starttime[6:8])
                        s_turnto_min=s_hour*60+s_min
                        endtime=line['WIC']['A'][s]['e']
                        e_hour=int(endtime[0:2])
                        e_min=int(endtime[3:5])
                        e_sec=int(endtime[6:8])
                        e_turnto_min=e_hour*60+e_min
                        inter_min=e_turnto_min-s_turnto_min
                        if inter_min>0:
                                left_border=fractions.Fraction(60-s_sec,60)
                                right_border=fractions.Fraction(e_sec,60)
                                # left border 7/10
                                # right_border 1/20
                                if 60-s_sec>30:
                                    array[s_turnto_min]+=left_border
                                if e_sec>30:
                                    array[e_turnto_min]+=right_border
                                for m in range(1,inter_min):
                                    # add 1  1350,add 1  1351, add 1  1352
                                    array[s_turnto_min+m]+=1
                                    # print array
                        else:
                            if e_sec-s_sec>=30:
                                num=fractions.Fraction(e_sec-s_sec,60)
                                array[e_turnto_min]+=num
            else:
                # if line['WIC']['A']['p']=='%E5%A5%94%E8%B7%91%E5%90%A7%E5%85%84%E5%BC%9F':
                    if line['WIC']['A']['sn'].encode('utf-8')== one_channel:
                        starttime=line['WIC']['A']['s']
                        s_hour=int(starttime[0:2])
                        s_min=int(starttime[3:5])
                        s_sec=int(starttime[6:8])
                        s_turnto_min=s_hour*60+s_min
                        # print starttime,s_hour,s_min,s_sec
                        endtime=line['WIC']['A']['e']
                        e_hour=int(endtime[0:2])
                        e_min=int(endtime[3:5])
                        e_sec=int(endtime[6:8])
                        e_turnto_min=e_hour*60+e_min
                        # print endtime,endtime
                        inter_min=e_turnto_min-s_turnto_min
                        if inter_min>0:
                            left_border=fractions.Fraction(60-s_sec,60)
                            right_border=fractions.Fraction(e_sec,60)
                            # left border 7/10
                            # right_border 1/20
                            if 60-s_sec>30:
                                array[s_turnto_min]+=left_border
                            if e_sec>30:
                                array[e_turnto_min]+=right_border
                            for m in range(1,inter_min):
                                # add 1  1350,add 1  1351, add 1  1352
                                array[s_turnto_min+m]+=1
                        else:
                            if e_sec-s_sec>=30:
                                num=fractions.Fraction(e_sec-s_sec,60)
                                array[e_turnto_min]+=num
    except:
        array = [0] * 1440

    array = np.array(array)
    a = list(array)
    #
    sample_number = float(50000)/float(1.98)
    try:
        a = [ '%.3f' % (elem*100/sample_number) for elem in a]
    except:
        a = [0] * 1440

    # 插入数据库！！！！！！！！！！！！！！！！！！！！！！！！
    count = 0

    for element in a:
        insertList = list()
        tempInsert = list()

        tempInsert.append(date)
        if count > 60:
        # yu is min,chu_shu is hour
            yu = count % 60
            # print yu
            chu_shu = count / 60
            # print chu_shu
            if len(str(yu)) < 2:
                yu = '0'+str(yu)
            else:
                yu = str(yu)
            if len(str(chu_shu)) < 2:
                chu_shu = '0'+str(chu_shu)
            else:
                chu_shu = str(chu_shu)
            period = chu_shu+yu
        else:
            if len(str(count)) < 2:

                period = '000'+str(count)
            else:
                period = '00'+str(count)
        tempInsert.append(period)
        tempInsert.append(one_channel)
        tempInsert.append(element)
        insertList.append(tuple(tempInsert))
        if count1>=10:
            mysqlcursor.executemany("insert into audienceRate_min_teeee(date_time, minute_time, channel, tvRating) values (%s, %s, %s, %s)" , (insertList))
            mysqlconn.commit()
            count += 1
            count1 = 0
    mysqlcursor.executemany("insert into audienceRate_min_teeee(date_time, minute_time, channel, tvRating) values (%s, %s, %s, %s)" , (insertList))
    mysqlconn.commit()
    count += 1
    print one_channel,"已经完成，目前完成",channel_no
end = datetime.datetime.now()

interval = end - start
print "共耗时：",interval

mysqlcursor.close()
mysqlconn.close()
conn.close()

# ！！！！！！！！！！！！！！！！！！！！！所有节目的收视率！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
# lines=eval("iae_audiencerate_new.find({'WIC.date':'%s'}).batch_size(30)" %('2014-12-05'))
# for line in lines:
#     try:
#         L=len(line['WIC']['A'])
#
#         if type(line['WIC']['A']) is list:
#             for s in range(0,L):
#                 starttime=line['WIC']['A'][s]['s']
#                 s_hour=int(starttime[0:2])
#                 s_min=int(starttime[3:5])
#                 s_sec=int(starttime[6:8])
#                 s_turnto_min=s_hour*60+s_min
#                 endtime=line['WIC']['A'][s]['e']
#                 e_hour=int(endtime[0:2])
#                 e_min=int(endtime[3:5])
#                 e_sec=int(endtime[6:8])
#                 e_turnto_min=e_hour*60+e_min
#                 inter_min=e_turnto_min-s_turnto_min
#                 if inter_min>0:
#                         left_border=fractions.Fraction(60-s_sec,60)
#                         right_border=fractions.Fraction(e_sec,60)
#                         # left border 7/10
#                         # right_border 1/20
#                         if 60-s_sec>30:
#                             array[s_turnto_min]+=left_border
#                         if e_sec>30:
#                             array[e_turnto_min]+=right_border
#                         for m in range(1,inter_min):
#                             # add 1  1350,add 1  1351, add 1  1352
#                             array[s_turnto_min+m]+=1
#                 else:
#                     if e_sec-s_sec>=30:
#                         num=fractions.Fraction(e_sec-s_sec,60)
#                         array[e_turnto_min]+=num
#         else:
#             starttime=line['WIC']['A']['s']
#             s_hour=int(starttime[0:2])
#             s_min=int(starttime[3:5])
#             s_sec=int(starttime[6:8])
#             s_turnto_min=s_hour*60+s_min
#             # print starttime,s_hour,s_min,s_sec
#             endtime=line['WIC']['A']['e']
#             e_hour=int(endtime[0:2])
#             e_min=int(endtime[3:5])
#             e_sec=int(endtime[6:8])
#             e_turnto_min=e_hour*60+e_min
#             # print endtime,endtime
#             inter_min=e_turnto_min-s_turnto_min
#             if inter_min>0:
#                 left_border=fractions.Fraction(60-s_sec,60)
#                 right_border=fractions.Fraction(e_sec,60)
#                 # left border 7/10
#                 # right_border 1/20
#                 if 60-s_sec>30:
#                     array[s_turnto_min]+=left_border
#                 if e_sec>30:
#                     array[e_turnto_min]+=right_border
#                 for m in range(1,inter_min):
#                     # add 1  1350,add 1  1351, add 1  1352
#                     array[s_turnto_min+m]+=1
#             else:
#                 if e_sec-s_sec>=30:
#                     num=fractions.Fraction(e_sec-s_sec,60)
#                     array[e_turnto_min]+=num
#     except:
#         continue
#
# print "array",array



# array [Fraction(16037, 30), Fraction(27923, 15), Fraction(190457, 60), Fraction(266923, 60), Fraction(171509, 30), Fraction(372007, 60), Fraction(92629, 15), Fraction(367351, 60), Fraction(18316, 3), Fraction(72599, 12), Fraction(363371, 60), Fraction(89989, 15), Fraction(59311, 10), Fraction(354889, 60), Fraction(87967, 15), Fraction(175663, 30), Fraction(347597, 60), Fraction(33683, 6), Fraction(53963, 10), Fraction(51193, 10), Fraction(71666, 15), Fraction(69542, 15), Fraction(13652, 3), Fraction(55127, 12), Fraction(70658, 15), Fraction(148253, 30), Fraction(50767, 10), Fraction(15367, 3), Fraction(101237, 20), Fraction(291941, 60), Fraction(136703, 30), Fraction(4299, 1), Fraction(62158, 15), Fraction(120743, 30), Fraction(235069, 60), Fraction(3906, 1), Fraction(117781, 30), Fraction(48779, 12), Fraction(50537, 12), Fraction(262159, 60), Fraction(55381, 12), Fraction(283297, 60), Fraction(23963, 5), Fraction(9517, 2), Fraction(13861, 3), Fraction(13124, 3), Fraction(249643, 60), Fraction(123683, 30), Fraction(61988, 15), Fraction(126763, 30), Fraction(268069, 60), Fraction(91427, 20), Fraction(9383, 2), Fraction(13966, 3), Fraction(269161, 60), Fraction(128383, 30), Fraction(61036, 15), Fraction(121613, 30), Fraction(81753, 20), Fraction(50105, 12), Fraction(131653, 30), Fraction(272383, 60), Fraction(47183, 10), Fraction(289429, 60), Fraction(48217, 10), Fraction(14504, 3), Fraction(287179, 60), Fraction(14480, 3), Fraction(48291, 10), Fraction(71761, 15), Fraction(57469, 12), Fraction(70517, 15), Fraction(92929, 20), Fraction(45273, 10), Fraction(12998, 3), Fraction(245389, 60), Fraction(231439, 60), Fraction(114773, 30), Fraction(115229, 30), Fraction(236173, 60), Fraction(250679, 60), Fraction(51619, 12), Fraction(4390, 1), Fraction(261643, 60), Fraction(20998, 5), Fraction(238081, 60), Fraction(226463, 60), Fraction(75007, 20), Fraction(75601, 20), Fraction(46927, 12), Fraction(246121, 60), Fraction(50801, 12), Fraction(17299, 4), Fraction(128869, 30), Fraction(248053, 60), Fraction(235999, 60), Fraction(74947, 20), Fraction(44719, 12), Fraction(37971, 10), Fraction(58759, 15), Fraction(246271, 60), Fraction(256207, 60), Fraction(66167, 15), Fraction(26957, 6), Fraction(22558, 5), Fraction(67772, 15), Fraction(4463, 1), Fraction(67499, 15), Fraction(45139, 10), Fraction(67336, 15), Fraction(89781, 20), Fraction(88557, 20), Fraction(267679, 60), Fraction(268207, 60), Fraction(89039, 20), Fraction(88779, 20), Fraction(86937, 20), Fraction(64223, 15), Fraction(41773, 10), Fraction(79913, 20), Fraction(75841, 20), Fraction(216659, 60), Fraction(35859, 10), Fraction(72519, 20), Fraction(56087, 15), Fraction(236171, 60), Fraction(121061, 30), Fraction(40961, 10), Fraction(4070, 1), Fraction(235583, 60), Fraction(56012, 15), Fraction(212177, 60), Fraction(210253, 60), Fraction(70869, 20), Fraction(109511, 30), Fraction(3868, 1), Fraction(118859, 30), Fraction(243929, 60), Fraction(20328, 5), Fraction(117919, 30), Fraction(225037, 60), Fraction(71381, 20), Fraction(211619, 60), Fraction(106337, 30), Fraction(14581, 4), Fraction(45973, 12), Fraction(118373, 30), Fraction(40219, 10), Fraction(40003, 10), Fraction(77349, 20), Fraction(73571, 20), Fraction(102901, 30), Fraction(49718, 15), Fraction(16284, 5), Fraction(191443, 60), Fraction(16049, 5), Fraction(47894, 15), Fraction(193777, 60), Fraction(48316, 15), Fraction(192793, 60), Fraction(48017, 15), Fraction(18931, 6), Fraction(15947, 5), Fraction(19387, 6), Fraction(193787, 60), Fraction(12895, 4), Fraction(191539, 60), Fraction(13181, 4), Fraction(102193, 30), Fraction(213761, 60), Fraction(56683, 15), Fraction(15717, 4), Fraction(245341, 60), Fraction(251369, 60), Fraction(42159, 10), Fraction(254537, 60), Fraction(125417, 30), Fraction(126553, 30), Fraction(126973, 30), Fraction(16883, 4), Fraction(253721, 60), Fraction(122393, 30), Fraction(19861, 5), Fraction(45127, 12), Fraction(205289, 60), Fraction(181673, 60), Fraction(159761, 60), Fraction(12813, 5), Fraction(158743, 60), Fraction(34519, 12), Fraction(196147, 60), Fraction(10706, 3), Fraction(7419, 2), Fraction(43709, 12), Fraction(20249, 6), Fraction(91139, 30), Fraction(13294, 5), Fraction(2506, 1), Fraction(2470, 1), Fraction(76183, 30), Fraction(8122, 3), Fraction(28297, 10), Fraction(17381, 6), Fraction(11607, 4), Fraction(165649, 60), Fraction(154031, 60), Fraction(11978, 5), Fraction(48819, 20), Fraction(10337, 4), Fraction(43144, 15), Fraction(13069, 4), Fraction(14277, 4), Fraction(45563, 12), Fraction(230857, 60), Fraction(11234, 3), Fraction(106511, 30), Fraction(13437, 4), Fraction(99547, 30), Fraction(50653, 15), Fraction(17427, 5), Fraction(55418, 15), Fraction(224257, 60), Fraction(18754, 5), Fraction(14543, 4), Fraction(200479, 60), Fraction(89227, 30), Fraction(51499, 20), Fraction(139901, 60), Fraction(32566, 15), Fraction(126403, 60), Fraction(31444, 15), Fraction(20891, 10), Fraction(33074, 15), Fraction(23181, 10), Fraction(146471, 60), Fraction(39521, 15), Fraction(169519, 60), Fraction(45463, 15), Fraction(95593, 30), Fraction(198479, 60), Fraction(33923, 10), Fraction(13467, 4), Fraction(40865, 12), Fraction(200939, 60), Fraction(194983, 60), Fraction(187417, 60), Fraction(44108, 15), Fraction(42964, 15), Fraction(41237, 15), Fraction(31481, 12), Fraction(148013, 60), Fraction(34633, 15), Fraction(134131, 60), Fraction(127321, 60), Fraction(24137, 12), Fraction(54841, 30), Fraction(5051, 3), Fraction(1593, 1), Fraction(5903, 4), Fraction(80951, 60), Fraction(5956, 5), Fraction(3190, 3), Fraction(57019, 60), Fraction(50321, 60), Fraction(42523, 60), Fraction(16781, 30), Fraction(6376, 15), Fraction(5081, 15), Fraction(16501, 60), Fraction(14347, 60), Fraction(13511, 60), Fraction(449, 2), Fraction(2279, 10), Fraction(1196, 5), Fraction(14137, 60), Fraction(7339, 30), Fraction(3659, 15), Fraction(7459, 30), Fraction(2479, 10), Fraction(243, 1), Fraction(3797, 15), Fraction(1242, 5), Fraction(7477, 30), Fraction(3788, 15), Fraction(5013, 20), Fraction(3722, 15), Fraction(3083, 12), Fraction(15571, 60), Fraction(3031, 12), Fraction(3751, 15), Fraction(2357, 10), Fraction(13633, 60), Fraction(13309, 60), Fraction(4283, 20), Fraction(3274, 15), Fraction(2527, 12), Fraction(6509, 30), Fraction(230, 1), Fraction(2453, 10), Fraction(3107, 12), Fraction(5487, 20), Fraction(1427, 5), Fraction(3607, 12), Fraction(18311, 60), Fraction(621, 2), Fraction(3171, 10), Fraction(19039, 60), Fraction(18833, 60), Fraction(4664, 15), Fraction(18467, 60), Fraction(8671, 30), Fraction(17027, 60), Fraction(8681, 30), Fraction(1751, 6), Fraction(18761, 60), Fraction(4996, 15), Fraction(7149, 20), Fraction(1847, 5), Fraction(5672, 15), Fraction(11687, 30), Fraction(777, 2), Fraction(23911, 60), Fraction(23641, 60), Fraction(4765, 12), Fraction(6197, 15), Fraction(4901, 12), Fraction(2491, 6), Fraction(5041, 12), Fraction(12967, 30), Fraction(13289, 30), Fraction(8693, 20), Fraction(4559, 10), Fraction(13931, 30), Fraction(27887, 60), Fraction(6949, 15), Fraction(2332, 5), Fraction(9693, 20), Fraction(2407, 5), Fraction(29603, 60), Fraction(10039, 20), Fraction(29917, 60), Fraction(30391, 60), Fraction(31069, 60), Fraction(15661, 30), Fraction(32401, 60), Fraction(32369, 60), Fraction(16273, 30), Fraction(3245, 6), Fraction(10709, 20), Fraction(10081, 20), Fraction(2409, 5), Fraction(27847, 60), Fraction(1907, 4), Fraction(2951, 6), Fraction(7741, 15), Fraction(34271, 60), Fraction(1819, 3), Fraction(18547, 30), Fraction(12757, 20), Fraction(18947, 30), Fraction(18151, 30), Fraction(6041, 10), Fraction(12357, 20), Fraction(18821, 30), Fraction(10183, 15), Fraction(3691, 5), Fraction(9521, 12), Fraction(10021, 12), Fraction(4292, 5), Fraction(12899, 15), Fraction(52463, 60), Fraction(18117, 20), Fraction(2813, 3), Fraction(56843, 60), Fraction(28943, 30), Fraction(58873, 60), Fraction(6029, 6), Fraction(31001, 30), Fraction(31369, 30), Fraction(31277, 30), Fraction(31619, 30), Fraction(12991, 12), Fraction(5347, 5), Fraction(61229, 60), Fraction(55607, 60), Fraction(3991, 5), Fraction(43361, 60), Fraction(4505, 6), Fraction(48443, 60), Fraction(5237, 6), Fraction(31523, 30), Fraction(35477, 30), Fraction(18754, 15), Fraction(12789, 10), Fraction(38467, 30), Fraction(18439, 15), Fraction(73363, 60), Fraction(75103, 60), Fraction(78329, 60), Fraction(6754, 5), Fraction(21677, 15), Fraction(45913, 30), Fraction(47011, 30), Fraction(1595, 1), Fraction(23488, 15), Fraction(4570, 3), Fraction(89821, 60), Fraction(23036, 15), Fraction(48253, 30), Fraction(50803, 30), Fraction(110653, 60), Fraction(38731, 20), Fraction(2002, 1), Fraction(24359, 12), Fraction(118889, 60), Fraction(116891, 60), Fraction(37007, 20), Fraction(5410, 3), Fraction(16999, 10), Fraction(16241, 10), Fraction(91529, 60), Fraction(22502, 15), Fraction(6621, 4), Fraction(5513, 3), Fraction(124367, 60), Fraction(24041, 10), Fraction(8078, 3), Fraction(43286, 15), Fraction(44837, 15), Fraction(31273, 10), Fraction(191677, 60), Fraction(190579, 60), Fraction(95759, 30), Fraction(46414, 15), Fraction(35951, 12), Fraction(5803, 2), Fraction(84709, 30), Fraction(86551, 30), Fraction(176617, 60), Fraction(6219, 2), Fraction(50231, 15), Fraction(10633, 3), Fraction(74099, 20), Fraction(11279, 3), Fraction(38337, 10), Fraction(46751, 12), Fraction(57971, 15), Fraction(225677, 60), Fraction(214223, 60), Fraction(196571, 60), Fraction(5613, 2), Fraction(26789, 10), Fraction(163769, 60), Fraction(28379, 10), Fraction(190733, 60), Fraction(3662, 1), Fraction(60781, 15), Fraction(17263, 4), Fraction(67216, 15), Fraction(275203, 60), Fraction(138799, 30), Fraction(93823, 20), Fraction(143293, 30), Fraction(287083, 60), Fraction(289903, 60), Fraction(29065, 6), Fraction(73733, 15), Fraction(300721, 60), Fraction(60313, 12), Fraction(15230, 3), Fraction(30647, 6), Fraction(25747, 5), Fraction(26076, 5), Fraction(314813, 60), Fraction(313543, 60), Fraction(316511, 60), Fraction(106319, 20), Fraction(324433, 60), Fraction(53947, 10), Fraction(65123, 12), Fraction(27252, 5), Fraction(334079, 60), Fraction(114101, 20), Fraction(114127, 20), Fraction(68911, 12), Fraction(345677, 60), Fraction(23321, 4), Fraction(11743, 2), Fraction(5916, 1), Fraction(118627, 20), Fraction(357049, 60), Fraction(180583, 30), Fraction(364259, 60), Fraction(91169, 15), Fraction(121691, 20), Fraction(18292, 3), Fraction(366337, 60), Fraction(185479, 30), Fraction(24761, 4), Fraction(372917, 60), Fraction(374753, 60), Fraction(25239, 4), Fraction(379691, 60), Fraction(190483, 30), Fraction(76339, 12), Fraction(63943, 10), Fraction(64109, 10), Fraction(193781, 30), Fraction(128561, 20), Fraction(192721, 30), Fraction(381043, 60), Fraction(127727, 20), Fraction(387121, 60), Fraction(388427, 60), Fraction(130547, 20), Fraction(393407, 60), Fraction(33014, 5), Fraction(66471, 10), Fraction(398969, 60), Fraction(401399, 60), Fraction(80219, 12), Fraction(201907, 30), Fraction(135519, 20), Fraction(26983, 4), Fraction(81649, 12), Fraction(406069, 60), Fraction(81389, 12), Fraction(136943, 20), Fraction(411421, 60), Fraction(413669, 60), Fraction(68253, 10), Fraction(80761, 12), Fraction(400271, 60), Fraction(25521, 4), Fraction(37099, 6), Fraction(348509, 60), Fraction(27877, 5), Fraction(27877, 5), Fraction(333767, 60), Fraction(345977, 60), Fraction(60613, 10), Fraction(188993, 30), Fraction(394387, 60), Fraction(80353, 12), Fraction(101867, 15), Fraction(409193, 60), Fraction(82561, 12), Fraction(414529, 60), Fraction(138429, 20), Fraction(104506, 15), Fraction(6971, 1), Fraction(411089, 60), Fraction(132269, 20), Fraction(371491, 60), Fraction(340373, 60), Fraction(151109, 30), Fraction(13777, 3), Fraction(268643, 60), Fraction(13799, 3), Fraction(50067, 10), Fraction(112729, 20), Fraction(184999, 30), Fraction(192701, 30), Fraction(128941, 20), Fraction(128041, 20), Fraction(121581, 20), Fraction(352591, 60), Fraction(349379, 60), Fraction(116787, 20), Fraction(18230, 3), Fraction(128119, 20), Fraction(133501, 20), Fraction(138021, 20), Fraction(420247, 60), Fraction(142889, 20), Fraction(142397, 20), Fraction(143303, 20), Fraction(216881, 30), Fraction(431963, 60), Fraction(145379, 20), Fraction(14493, 2), Fraction(145191, 20), Fraction(435121, 60), Fraction(144487, 20), Fraction(28917, 4), Fraction(108193, 15), Fraction(43195, 6), Fraction(21092, 3), Fraction(80915, 12), Fraction(390257, 60), Fraction(61513, 10), Fraction(23897, 4), Fraction(59627, 10), Fraction(72779, 12), Fraction(126359, 20), Fraction(397183, 60), Fraction(137703, 20), Fraction(418771, 60), Fraction(410683, 60), Fraction(66667, 10), Fraction(126703, 20), Fraction(61361, 10), Fraction(91828, 15), Fraction(12431, 2), Fraction(38575, 6), Fraction(40261, 6), Fraction(422839, 60), Fraction(73097, 10), Fraction(448013, 60), Fraction(226157, 30), Fraction(450679, 60), Fraction(445159, 60), Fraction(108347, 15), Fraction(33932, 5), Fraction(371339, 60), Fraction(53961, 10), Fraction(73756, 15), Fraction(47979, 10), Fraction(58783, 12), Fraction(160861, 30), Fraction(183407, 30), Fraction(33908, 5), Fraction(87055, 12), Fraction(225967, 30), Fraction(77731, 10), Fraction(77693, 10), Fraction(93397, 12), Fraction(94139, 12), Fraction(156421, 20), Fraction(470387, 60), Fraction(156609, 20), Fraction(472919, 60), Fraction(158811, 20), Fraction(119144, 15), Fraction(159369, 20), Fraction(15907, 2), Fraction(156147, 20), Fraction(151053, 20), Fraction(141619, 20), Fraction(385139, 60), Fraction(84878, 15), Fraction(15596, 3), Fraction(304913, 60), Fraction(312971, 60), Fraction(34189, 6), Fraction(129761, 20), Fraction(427291, 60), Fraction(89663, 12), Fraction(15007, 2), Fraction(444239, 60), Fraction(211037, 30), Fraction(68469, 10), Fraction(136827, 20), Fraction(34748, 5), Fraction(434503, 60), Fraction(454777, 60), Fraction(477137, 60), Fraction(492529, 60), Fraction(167547, 20), Fraction(126932, 15), Fraction(42239, 5), Fraction(256841, 30), Fraction(259271, 30), Fraction(519533, 60), Fraction(17311, 2), Fraction(518141, 60), Fraction(261379, 30), Fraction(87097, 10), Fraction(131303, 15), Fraction(524821, 60), Fraction(174899, 20), Fraction(88017, 10), Fraction(534601, 60), Fraction(535687, 60), Fraction(107867, 12), Fraction(89989, 10), Fraction(271327, 30), Fraction(547963, 60), Fraction(91509, 10), Fraction(552139, 60), Fraction(184169, 20), Fraction(276713, 30), Fraction(559993, 60), Fraction(139336, 15), Fraction(562231, 60), Fraction(140452, 15), Fraction(188973, 20), Fraction(190609, 20), Fraction(571813, 60), Fraction(192123, 20), Fraction(94991, 10), Fraction(115651, 12), Fraction(291893, 30), Fraction(97739, 10), Fraction(293819, 30), Fraction(587473, 60), Fraction(118621, 12), Fraction(199571, 20), Fraction(602123, 60), Fraction(201989, 20), Fraction(605153, 60), Fraction(602587, 60), Fraction(19867, 2), Fraction(145042, 15), Fraction(279551, 30), Fraction(266557, 30), Fraction(517067, 60), Fraction(127277, 15), Fraction(174153, 20), Fraction(135598, 15), Fraction(9519, 1), Fraction(585043, 60), Fraction(145201, 15), Fraction(92997, 10), Fraction(523381, 60), Fraction(116519, 15), Fraction(422077, 60), Fraction(410821, 60), Fraction(35749, 5), Fraction(234407, 30), Fraction(533591, 60), Fraction(199707, 20), Fraction(162343, 15), Fraction(56734, 5), Fraction(69667, 6), Fraction(349193, 30), Fraction(234393, 20), Fraction(356357, 30), Fraction(712769, 60), Fraction(178883, 15), Fraction(356801, 30), Fraction(358039, 30), Fraction(725789, 60), Fraction(60561, 5), Fraction(730499, 60), Fraction(724681, 60), Fraction(720359, 60), Fraction(732067, 60), Fraction(733439, 60), Fraction(184498, 15), Fraction(734903, 60), Fraction(73007, 6), Fraction(738611, 60), Fraction(73465, 6), Fraction(735973, 60), Fraction(183029, 15), Fraction(730229, 60), Fraction(366859, 30), Fraction(727703, 60), Fraction(48975, 4), Fraction(735199, 60), Fraction(73711, 6), Fraction(742609, 60), Fraction(184574, 15), Fraction(372733, 30), Fraction(743227, 60), Fraction(185758, 15), Fraction(186227, 15), Fraction(369967, 30), Fraction(148603, 12), Fraction(49361, 4), Fraction(736849, 60), Fraction(73451, 6), Fraction(146339, 12), Fraction(36823, 3), Fraction(183457, 15), Fraction(733037, 60), Fraction(244571, 20), Fraction(365353, 30), Fraction(724771, 60), Fraction(363641, 30), Fraction(364447, 30), Fraction(363547, 30), Fraction(720853, 60), Fraction(718429, 60), Fraction(59289, 5), Fraction(232253, 20), Fraction(222953, 20), Fraction(154783, 15), Fraction(56743, 6), Fraction(490753, 60), Fraction(224347, 30), Fraction(145063, 20), Fraction(111421, 15), Fraction(237847, 30), Fraction(8935, 1), Fraction(199137, 20), Fraction(317681, 30), Fraction(109533, 10), Fraction(671527, 60), Fraction(332293, 30), Fraction(666649, 60), Fraction(332657, 30), Fraction(54897, 5), Fraction(164816, 15), Fraction(54576, 5), Fraction(164921, 15), Fraction(54846, 5), Fraction(109067, 10), Fraction(327373, 30), Fraction(162754, 15), Fraction(650891, 60), Fraction(43225, 4), Fraction(214839, 20), Fraction(161284, 15), Fraction(642847, 60), Fraction(319331, 30), Fraction(315413, 30), Fraction(30391, 3), Fraction(578417, 60), Fraction(544349, 60), Fraction(87453, 10), Fraction(520871, 60), Fraction(104611, 12), Fraction(44886, 5), Fraction(47113, 5), Fraction(590873, 60), Fraction(20369, 2), Fraction(155269, 15), Fraction(624397, 60), Fraction(51469, 5), Fraction(616787, 60), Fraction(20569, 2), Fraction(616361, 60), Fraction(601213, 60), Fraction(201107, 20), Fraction(202463, 20), Fraction(101507, 10), Fraction(39889, 4), Fraction(39819, 4), Fraction(596233, 60), Fraction(199999, 20), Fraction(602407, 60), Fraction(596413, 60), Fraction(592033, 60), Fraction(19675, 2), Fraction(197357, 20), Fraction(19681, 2), Fraction(59155, 6), Fraction(118025, 12), Fraction(196049, 20), Fraction(39189, 4), Fraction(115393, 12), Fraction(37407, 4), Fraction(266849, 30), Fraction(125908, 15), Fraction(80687, 10), Fraction(159267, 20), Fraction(160117, 20), Fraction(124898, 15), Fraction(132418, 15), Fraction(548023, 60), Fraction(272057, 30), Fraction(260947, 30), Fraction(160413, 20), Fraction(422051, 60), Fraction(19408, 3), Fraction(125467, 20), Fraction(377261, 60), Fraction(68591, 10), Fraction(46645, 6), Fraction(513643, 60), Fraction(45873, 5), Fraction(571811, 60), Fraction(194289, 20), Fraction(194251, 20), Fraction(584927, 60), Fraction(292777, 30), Fraction(581819, 60), Fraction(581311, 60), Fraction(289937, 30), Fraction(291481, 30), Fraction(194649, 20), Fraction(57593, 6), Fraction(144631, 15), Fraction(192737, 20), Fraction(580609, 60), Fraction(194133, 20), Fraction(579293, 60), Fraction(48342, 5), Fraction(578003, 60), Fraction(38409, 4), Fraction(113791, 12), Fraction(135904, 15), Fraction(263107, 30), Fraction(165467, 20), Fraction(94595, 12), Fraction(30307, 4), Fraction(219601, 30), Fraction(433927, 60), Fraction(71963, 10), Fraction(435319, 60), Fraction(437981, 60), Fraction(108002, 15), Fraction(108332, 15), Fraction(435821, 60), Fraction(146803, 20), Fraction(151591, 20), Fraction(117413, 15), Fraction(492799, 60), Fraction(130519, 15), Fraction(45673, 5), Fraction(284407, 30), Fraction(115457, 12), Fraction(146443, 15), Fraction(196139, 20), Fraction(118237, 12), Fraction(592081, 60), Fraction(587423, 60), Fraction(195573, 20), Fraction(48601, 5), Fraction(29390, 3), Fraction(49297, 5), Fraction(98517, 10), Fraction(590573, 60), Fraction(117187, 12), Fraction(49164, 5), Fraction(9814, 1), Fraction(588571, 60), Fraction(58891, 6), Fraction(586769, 60), Fraction(592067, 60), Fraction(148081, 15), Fraction(296887, 30), Fraction(197921, 20), Fraction(296699, 30), Fraction(99277, 10), Fraction(149279, 15), Fraction(119609, 12), Fraction(119879, 12), Fraction(596173, 60), Fraction(29927, 3), Fraction(40057, 4), Fraction(600533, 60), Fraction(297373, 30), Fraction(198287, 20), Fraction(39651, 4), Fraction(298873, 30), Fraction(119839, 12), Fraction(602401, 60), Fraction(150211, 15), Fraction(600787, 60), Fraction(603139, 60), Fraction(50448, 5), Fraction(60725, 6), Fraction(606709, 60), Fraction(151843, 15), Fraction(50743, 5), Fraction(30232, 3), Fraction(151564, 15), Fraction(606799, 60), Fraction(609161, 60), Fraction(121501, 12), Fraction(607837, 60), Fraction(607277, 60), Fraction(607693, 60), Fraction(40841, 4), Fraction(204067, 20), Fraction(203789, 20), Fraction(615671, 60), Fraction(307417, 30), Fraction(103421, 10), Fraction(123383, 12), Fraction(205673, 20), Fraction(206283, 20), Fraction(154022, 15), Fraction(122837, 12), Fraction(605269, 60), Fraction(19599, 2), Fraction(47021, 5), Fraction(535153, 60), Fraction(52177, 6), Fraction(51077, 6), Fraction(8644, 1), Fraction(44811, 5), Fraction(56999, 6), Fraction(198903, 20), Fraction(102993, 10), Fraction(210887, 20), Fraction(641011, 60), Fraction(641527, 60), Fraction(643021, 60), Fraction(213839, 20), Fraction(214721, 20), Fraction(215147, 20), Fraction(641623, 60), Fraction(634069, 60), Fraction(623951, 60), Fraction(60623, 6), Fraction(39071, 4), Fraction(549853, 60), Fraction(535789, 60), Fraction(534139, 60), Fraction(180193, 20), Fraction(112301, 12), Fraction(49427, 5), Fraction(41451, 4), Fraction(53494, 5), Fraction(218883, 20), Fraction(332999, 30), Fraction(33013, 3), Fraction(332609, 30), Fraction(333071, 30), Fraction(111413, 10), Fraction(133831, 12), Fraction(167606, 15), Fraction(168922, 15), Fraction(224711, 20), Fraction(340231, 30), Fraction(228553, 20), Fraction(682651, 60), Fraction(342761, 30), Fraction(138097, 12), Fraction(57646, 5), Fraction(695701, 60), Fraction(231689, 20), Fraction(695173, 60), Fraction(697547, 60), Fraction(35020, 3), Fraction(707947, 60), Fraction(703907, 60), Fraction(235721, 20), Fraction(238649, 20), Fraction(359813, 30), Fraction(725677, 60), Fraction(144605, 12), Fraction(242551, 20), Fraction(365969, 30), Fraction(369479, 30), Fraction(742547, 60), Fraction(742601, 60), Fraction(747347, 60), Fraction(49977, 4), Fraction(754483, 60), Fraction(762061, 60), Fraction(761089, 60), Fraction(76271, 6), Fraction(191747, 15), Fraction(762211, 60), Fraction(773257, 60), Fraction(38522, 3), Fraction(77713, 6), Fraction(256203, 20), Fraction(51939, 4), Fraction(263809, 20), Fraction(78989, 6), Fraction(265137, 20), Fraction(794947, 60), Fraction(133851, 10), Fraction(162563, 12), Fraction(407311, 30), Fraction(822931, 60), Fraction(821563, 60), Fraction(41461, 3), Fraction(208054, 15), Fraction(277981, 20), Fraction(281097, 20), Fraction(84151, 6), Fraction(141093, 10), Fraction(212993, 15), Fraction(852269, 60), Fraction(426461, 30), Fraction(410617, 30), Fraction(40268, 3), Fraction(129697, 10), Fraction(184033, 15), Fraction(240083, 20), Fraction(177976, 15), Fraction(72851, 6), Fraction(760349, 60), Fraction(160061, 12), Fraction(210956, 15), Fraction(432253, 30), Fraction(58939, 4), Fraction(449693, 30), Fraction(894043, 60), Fraction(226678, 15), Fraction(45304, 3), Fraction(913681, 60), Fraction(46024, 3), Fraction(182147, 12), Fraction(929303, 60), Fraction(46574, 3), Fraction(939487, 60), Fraction(315013, 20), Fraction(467141, 30), Fraction(15812, 1), Fraction(95401, 6), Fraction(480649, 30), Fraction(241361, 15), Fraction(956881, 60), Fraction(161457, 10), Fraction(242834, 15), Fraction(323009, 20), Fraction(48892, 3), Fraction(486559, 30), Fraction(488719, 30), Fraction(159823, 10), Fraction(930211, 60), Fraction(89897, 6), Fraction(849479, 60), Fraction(41443, 3), Fraction(204829, 15), Fraction(415933, 30), Fraction(171301, 12), Fraction(89543, 6), Fraction(158459, 10), Fraction(983311, 60), Fraction(168071, 10), Fraction(1017517, 60), Fraction(254933, 15), Fraction(1034287, 60), Fraction(1047197, 60), Fraction(87783, 5), Fraction(351637, 20), Fraction(1051219, 60), Fraction(1062329, 60), Fraction(1067053, 60), Fraction(1068871, 60), Fraction(178371, 10), Fraction(177419, 10), Fraction(357597, 20), Fraction(358303, 20), Fraction(1075747, 60), Fraction(270071, 15), Fraction(522419, 30), Fraction(543271, 30), Fraction(1083299, 60), Fraction(266101, 15), Fraction(1035731, 60), Fraction(243589, 15), Fraction(463729, 30), Fraction(154121, 10), Fraction(468047, 30), Fraction(96293, 6), Fraction(82626, 5), Fraction(270767, 15), Fraction(367041, 20), Fraction(92578, 5), Fraction(95013, 5), Fraction(1134491, 60), Fraction(116867, 6), Fraction(1168343, 60), Fraction(291056, 15), Fraction(97888, 5), Fraction(1168541, 60), Fraction(1188629, 60), Fraction(1187533, 60), Fraction(294013, 15), Fraction(1181461, 60), Fraction(38759, 2), Fraction(39597, 2), Fraction(99343, 5), Fraction(1186177, 60), Fraction(59684, 3), Fraction(590401, 30), Fraction(400229, 20), Fraction(100072, 5), Fraction(597043, 30), Fraction(79943, 4), Fraction(118277, 6), Fraction(1175897, 60), Fraction(572087, 30), Fraction(71565, 4), Fraction(496541, 30), Fraction(877367, 60), Fraction(39344, 3), Fraction(12713, 1), Fraction(25865, 2), Fraction(833327, 60), Fraction(309697, 20), Fraction(17991, 1), Fraction(576761, 30), Fraction(1208653, 60), Fraction(201947, 10), Fraction(204551, 10), Fraction(416437, 20), Fraction(1249747, 60), Fraction(1228247, 60), Fraction(412131, 20), Fraction(1247887, 60), Fraction(1265777, 60), Fraction(126697, 6), Fraction(1251731, 60), Fraction(1253609, 60), Fraction(419891, 20), Fraction(417181, 20), Fraction(300638, 15), Fraction(1124953, 60), Fraction(256987, 15), Fraction(44560, 3), Fraction(809719, 60), Fraction(392933, 30), Fraction(799169, 60), Fraction(285807, 20), Fraction(82971, 5), Fraction(1127477, 60), Fraction(200051, 10), Fraction(1245379, 60), Fraction(626477, 30), Fraction(1274243, 60), Fraction(107107, 5), Fraction(1266211, 60), Fraction(255767, 12), Fraction(212653, 10), Fraction(213837, 10), Fraction(84795, 4), Fraction(20408, 1), Fraction(96108, 5), Fraction(87628, 5), Fraction(184213, 12), Fraction(835081, 60), Fraction(388357, 30), Fraction(75625, 6), Fraction(751039, 60), Fraction(828539, 60), Fraction(221623, 15), Fraction(466271, 30), Fraction(966491, 60), Fraction(960263, 60), Fraction(323083, 20), Fraction(321241, 20), Fraction(156153, 10), Fraction(904739, 60), Fraction(210416, 15), Fraction(78013, 6), Fraction(181919, 15), Fraction(230529, 20), Fraction(222873, 20), Fraction(312793, 30), Fraction(321079, 30), Fraction(44083, 4), Fraction(683221, 60), Fraction(708673, 60), Fraction(755387, 60), Fraction(832813, 60), Fraction(149991, 10), Fraction(235954, 15), Fraction(48031, 3), Fraction(320259, 20), Fraction(973253, 60), Fraction(242563, 15), Fraction(158637, 10), Fraction(910447, 60), Fraction(425609, 30), Fraction(260457, 20), Fraction(185944, 15), Fraction(730973, 60), Fraction(184526, 15), Fraction(773341, 60), Fraction(282461, 20), Fraction(925669, 60), Fraction(985007, 60), Fraction(17264, 1), Fraction(364511, 20), Fraction(384967, 20), Fraction(396781, 20), Fraction(60116, 3), Fraction(192137, 10), Fraction(532441, 30), Fraction(313559, 20), Fraction(66866, 5), Fraction(248733, 20), Fraction(336443, 30), Fraction(317639, 30), Fraction(642637, 60), Fraction(55556, 5), Fraction(717491, 60), Fraction(774823, 60), Fraction(147497, 10), Fraction(33621, 2), Fraction(278117, 15), Fraction(193971, 10), Fraction(1175053, 60), Fraction(385077, 20), Fraction(181337, 10), Fraction(1057279, 60), Fraction(1041167, 60), Fraction(1047721, 60), Fraction(1085419, 60), Fraction(1146451, 60), Fraction(239851, 12), Fraction(607339, 30), Fraction(1198667, 60), Fraction(1152367, 60), Fraction(363157, 20), Fraction(175561, 10), Fraction(174037, 10), Fraction(342761, 20), Fraction(359573, 20), Fraction(1134469, 60), Fraction(597253, 30), Fraction(1235407, 60), Fraction(1247591, 60), Fraction(420743, 20), Fraction(1250473, 60), Fraction(308768, 15), Fraction(1224883, 60), Fraction(1223359, 60), Fraction(245027, 12), Fraction(204379, 10), Fraction(408129, 20), Fraction(80887, 4), Fraction(80969, 4), Fraction(1217687, 60), Fraction(406721, 20), Fraction(1210543, 60), Fraction(1198039, 60), Fraction(1203901, 60), Fraction(200101, 10), Fraction(595457, 30), Fraction(589757, 30), Fraction(114737, 6), Fraction(107599, 6), Fraction(1108703, 60), Fraction(549311, 30), Fraction(1099363, 60), Fraction(177793, 10), Fraction(174319, 10), Fraction(354737, 20), Fraction(533863, 30), Fraction(177603, 10), Fraction(1054073, 60), Fraction(34619, 2), Fraction(261463, 15), Fraction(349893, 20), Fraction(523193, 30), Fraction(17360, 1), Fraction(102301, 6), Fraction(1032941, 60), Fraction(1036649, 60), Fraction(173019, 10), Fraction(513631, 30), Fraction(1006783, 60), Fraction(254507, 15), Fraction(84819, 5), Fraction(169013, 10), Fraction(83397, 5), Fraction(162603, 10), Fraction(163897, 10), Fraction(195709, 12), Fraction(162101, 10), Fraction(320039, 20), Fraction(186751, 12), Fraction(181621, 12), Fraction(897401, 60), Fraction(298523, 20), Fraction(74871, 5), Fraction(86405, 6), Fraction(144807, 10), Fraction(215402, 15), Fraction(852883, 60), Fraction(212713, 15), Fraction(278627, 20), Fraction(207308, 15), Fraction(277539, 20), Fraction(828611, 60), Fraction(137883, 10), Fraction(817717, 60), Fraction(270439, 20), Fraction(13500, 1), Fraction(399413, 30), Fraction(265761, 20), Fraction(782303, 60), Fraction(64121, 5), Fraction(192748, 15), Fraction(769529, 60), Fraction(255511, 20), Fraction(759367, 60), Fraction(188653, 15), Fraction(746171, 60), Fraction(37247, 3), Fraction(149033, 12), Fraction(366997, 30), Fraction(734723, 60), Fraction(120749, 10), Fraction(12069, 1), Fraction(180491, 15), Fraction(59628, 5), Fraction(118301, 10), Fraction(175817, 15), Fraction(701689, 60), Fraction(698621, 60), Fraction(230181, 20), Fraction(68393, 6), Fraction(167746, 15), Fraction(669409, 60), Fraction(670511, 60), Fraction(166198, 15), Fraction(54811, 5), Fraction(54413, 5), Fraction(216617, 20), Fraction(643193, 60), Fraction(125041, 12), Fraction(62255, 6), Fraction(613703, 60), Fraction(307543, 30), Fraction(101673, 10), Fraction(100481, 10), Fraction(118405, 12), Fraction(58763, 6), Fraction(29332, 3), Fraction(584741, 60), Fraction(287467, 30), Fraction(114571, 12), Fraction(563899, 60), Fraction(567103, 60), Fraction(140993, 15), Fraction(185423, 20), Fraction(553801, 60), Fraction(274313, 30), Fraction(183071, 20), Fraction(274231, 30), Fraction(135734, 15), Fraction(134308, 15), Fraction(176277, 20), Fraction(43708, 5), Fraction(130522, 15), Fraction(103549, 12), Fraction(51463, 6), Fraction(8408, 1), Fraction(252869, 30), Fraction(501589, 60), Fraction(249749, 30), Fraction(164761, 20), Fraction(122171, 15), Fraction(15769, 2), Fraction(87391, 12), Fraction(125689, 20), Fraction(151067, 30), Fraction(13635, 4), Fraction(46289, 20), Fraction(37219, 20), Fraction(122999, 60), Fraction(14257, 5), Fraction(110423, 30), Fraction(203561, 60), Fraction(168643, 60), Fraction(111749, 60), Fraction(12171, 20)]

# conn.close()