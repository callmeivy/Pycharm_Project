# coding=UTF-8
'''
Created on 25 Dec 2014
观众流入流出
@author: Jin
'''
import pymongo
from collections import defaultdict
import fractions
import numpy as np
conn=pymongo.Connection('172.16.168.45',27017)
iae_hitlog_record=conn.gehua.iae_hitlog_record
array = [0] * 1440
array2 = [0] * 1440
caid_can=list()
caid_time=defaultdict(list)
caid_time2=defaultdict(list)
caid_box=list()
# array [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, Fraction(431, 60)(Index:1265), Fraction(779, 20), Fraction(4411, 60), Fraction(1579, 15), Fraction(4753, 30), Fraction(668, 3), Fraction(1543, 6), Fraction(17279, 60), Fraction(18829, 60), Fraction(3147, 10), Fraction(3111, 10), Fraction(18773, 60), Fraction(3169, 10), Fraction(637, 2), Fraction(1013, 3), Fraction(5318, 15), Fraction(751, 2), Fraction(11429, 30), Fraction(3277, 10), Fraction(9611, 30), Fraction(9257, 30), Fraction(5971, 20), Fraction(18377, 60), Fraction(1617, 5), Fraction(7091, 20), Fraction(4633, 12), Fraction(1621, 4), Fraction(6467, 15), Fraction(8571, 20), Fraction(1345, 3), Fraction(13537, 30), Fraction(450, 1), Fraction(14473, 30), Fraction(9913, 20), Fraction(2562, 5), Fraction(15229, 30), Fraction(6209, 12), Fraction(1565, 3), Fraction(15767, 30), Fraction(10749, 20), Fraction(8182, 15), Fraction(8156, 15), Fraction(3349, 6), Fraction(8398, 15), Fraction(2249, 4), Fraction(2804, 5), Fraction(6697, 12), Fraction(27971, 60), Fraction(12863, 30), Fraction(2393, 5), Fraction(9929, 20), Fraction(9969, 20), Fraction(15361, 30), Fraction(6329, 12), Fraction(8663, 15), Fraction(37001, 60), Fraction(1273, 2), Fraction(9547, 15), Fraction(9356, 15), Fraction(12903, 20), Fraction(3907, 6), Fraction(13197, 20), Fraction(6587, 10), Fraction(19243, 30), Fraction(20039, 30), Fraction(20527, 30), Fraction(6987, 10), Fraction(13849, 20), Fraction(8087, 12), Fraction(10298, 15), Fraction(2045, 3), Fraction(2075, 3), Fraction(41239, 60), Fraction(40291, 60), Fraction(10342, 15), Fraction(42049, 60), Fraction(4241, 6), Fraction(10601, 15), Fraction(41057, 60), Fraction(21199, 30), Fraction(21407, 30), Fraction(43387, 60), Fraction(2183, 3), Fraction(42437, 60), Fraction(43457, 60), Fraction(10864, 15), Fraction(3651, 5), Fraction(43943, 60), Fraction(42673, 60), Fraction(45989, 60), Fraction(3125, 4), Fraction(11944, 15), Fraction(23929, 30), Fraction(46421, 60), Fraction(3929, 5), Fraction(7811, 10), Fraction(7573, 10), Fraction(11339, 15), Fraction(14549, 20), Fraction(3977, 6), Fraction(8806, 15), Fraction(34931, 60), Fraction(8726, 15), Fraction(33341, 60), Fraction(34877, 60), Fraction(19063, 30), Fraction(40859, 60), Fraction(10373, 15), Fraction(8191, 12), Fraction(14041, 20), Fraction(1373, 2), Fraction(1796, 3), Fraction(8837, 15), Fraction(17309, 30), Fraction(17507, 30), Fraction(34561, 60), Fraction(36497, 60), Fraction(3013, 5), Fraction(17033, 30), Fraction(25459, 60), Fraction(13523, 60), Fraction(1697, 12), Fraction(673, 10), Fraction(1423, 60), Fraction(371, 30), Fraction(9, 1), Fraction(9, 2), 1, 1, 0, 0, 0, 0, 0, 0, Fraction(47, 4), Fraction(1381, 20), Fraction(2293, 20), Fraction(2023, 12), Fraction(14473, 60), Fraction(15713, 60), Fraction(5579, 20), Fraction(4304, 15), Fraction(1437, 5), Fraction(17581, 60), Fraction(1693, 6), Fraction(17393, 60), Fraction(8731, 30), Fraction(8509, 30), Fraction(3445, 12), Fraction(3301, 12), Fraction(3692, 15), Fraction(3338, 15), Fraction(2659, 12), Fraction(13133, 60), Fraction(12929, 60), Fraction(4329, 20), Fraction(2635, 12), Fraction(6869, 30), Fraction(2881, 12), Fraction(963, 4), Fraction(1433, 6), Fraction(4479, 20), Fraction(11639, 60), Fraction(8731, 60), Fraction(6391, 60), Fraction(1177, 15), Fraction(3731, 60), Fraction(779, 10), Fraction(6593, 60), Fraction(7631, 60), Fraction(3439, 30), Fraction(2971, 30), Fraction(355, 6), Fraction(221, 20)]

# ！！！！！！！！！！！！！！！！！！！！！！！！！！！这是不重复观众数！！！！！！！！！！！！！！！！！
lines=eval("iae_hitlog_record.find({'date':'%s','channel_name':'%s'}).batch_size(30)" %('2014-12-06','浙江卫视'))
for line in lines:

    starttime=line['playRequestTime']
    caid=line['caid']
    s_hour=int(starttime[11:13])
    s_min=int(starttime[14:16])
    s_sec=int(starttime[17:19])
    s_turnto_min=s_hour*60+s_min
    endtime=line['endPlayTime']
    e_hour=int(endtime[11:13])
    e_min=int(endtime[14:16])
    e_sec=int(endtime[17:19])
    e_turnto_min=e_hour*60+e_min
    inter_min=e_turnto_min-s_turnto_min

    if s_turnto_min in caid_time:
        # caid_time是一个值为list的字典
        if caid not in caid_time[s_turnto_min]:
            caid_time[str(s_turnto_min)].append(str(caid))
            array[s_turnto_min]+=1
    else:
        caid_time[str(s_turnto_min)]=caid
        array[s_turnto_min]+=1
    # 处理右边边界
    if e_turnto_min in caid_time2:
        if caid not in caid_time2[e_turnto_min]:
            caid_time2[str(e_turnto_min)].append(str(caid))
            array2[e_turnto_min]+=1
    else:
        caid_time2[str(e_turnto_min)]=caid
        array2[e_turnto_min]+=1




print "array",array
print "array2",array2


# array [25, 17, 22, 33, 27, 30, 27, 32, 79, 42, 39, 35, 30, 30, 36, 32, 27, 40, 41, 41, 32, 28, 29, 41, 35, 29, 37, 24, 32, 25, 31, 35, 32, 21, 37, 25, 38, 24, 27, 26, 23, 12, 31, 22, 28, 28, 17, 25, 30, 30, 17, 17, 18, 16, 22, 17, 13, 17, 19, 17, 11, 11, 12, 50, 40, 26, 12, 19, 23, 18, 20, 15, 15, 34, 11, 14, 17, 18, 17, 32, 30, 19, 24, 15, 12, 16, 12, 14, 17, 14, 14, 10, 16, 19, 37, 24, 21, 26, 12, 14, 9, 8, 11, 15, 17, 10, 8, 8, 9, 14, 9, 9, 7, 14, 8, 14, 4, 9, 4, 7, 11, 6, 10, 11, 1, 13, 13, 5, 10, 7, 4, 9, 5, 10, 5, 10, 9, 10, 12, 12, 9, 8, 7, 4, 5, 6, 7, 11, 7, 6, 3, 6, 9, 6, 8, 5, 4, 7, 9, 7, 3, 5, 9, 3, 7, 7, 4, 3, 5, 5, 2, 4, 5, 4, 7, 3, 5, 2, 7, 4, 1, 4, 2, 8, 4, 5, 4, 5, 5, 4, 1, 3, 3, 2, 3, 2, 8, 5, 3, 3, 2, 4, 4, 4, 3, 3, 3, 2, 1, 3, 2, 2, 6, 1, 3, 4, 2, 3, 1, 3, 3, 3, 4, 1, 5, 5, 3, 1, 3, 5, 8, 4, 5, 2, 1, 5, 1, 1, 3, 3, 4, 5, 3, 5, 4, 2, 5, 4, 1, 2, 4, 2, 3, 1, 2, 3, 2, 2, 0, 4, 5, 5, 2, 6, 2, 1, 5, 4, 4, 1, 5, 3, 5, 2, 3, 2, 2, 0, 3, 1, 3, 5, 2, 2, 1, 3, 3, 5, 6, 1, 5, 2, 1, 3, 1, 1, 1, 3, 5, 2, 3, 1, 4, 2, 2, 3, 0, 3, 6, 1, 2, 4, 2, 3, 2, 6, 4, 1, 4, 1, 4, 7, 7, 1, 5, 3, 2, 2, 5, 3, 5, 3, 3, 3, 5, 2, 5, 4, 3, 4, 4, 5, 4, 4, 3, 5, 3, 1, 3, 1, 4, 5, 5, 3, 5, 2, 9, 4, 3, 5, 5, 4, 5, 12, 5, 7, 7, 8, 3, 4, 7, 5, 5, 8, 4, 3, 9, 12, 6, 7, 12, 16, 12, 12, 15, 14, 3, 11, 9, 7, 12, 10, 11, 13, 16, 13, 10, 13, 14, 9, 8, 14, 9, 19, 20, 17, 16, 21, 20, 12, 7, 15, 12, 13, 15, 13, 13, 18, 22, 25, 23, 13, 21, 23, 15, 22, 29, 22, 21, 27, 18, 36, 23, 26, 31, 29, 21, 36, 26, 27, 25, 23, 20, 38, 37, 31, 34, 36, 33, 45, 31, 28, 32, 47, 34, 40, 42, 43, 35, 55, 58, 47, 44, 44, 47, 50, 40, 48, 58, 64, 50, 50, 56, 59, 58, 67, 66, 50, 48, 59, 57, 57, 62, 66, 61, 67, 69, 63, 89, 66, 62, 84, 68, 75, 79, 83, 82, 77, 84, 91, 86, 77, 93, 70, 82, 72, 82, 89, 89, 87, 95, 96, 83, 96, 96, 90, 81, 96, 90, 100, 96, 107, 105, 114, 117, 103, 92, 100, 123, 116, 102, 123, 101, 98, 143, 123, 131, 133, 138, 98, 141, 119, 113, 102, 114, 133, 91, 122, 136, 103, 118, 127, 117, 112, 114, 101, 113, 117, 111, 120, 135, 121, 134, 129, 94, 128, 107, 110, 133, 124, 116, 129, 125, 127, 125, 120, 129, 126, 134, 127, 110, 112, 109, 118, 108, 125, 111, 104, 99, 127, 113, 112, 113, 103, 116, 106, 107, 97, 111, 97, 93, 89, 124, 134, 129, 96, 125, 113, 107, 109, 112, 131, 104, 96, 112, 107, 103, 123, 121, 140, 106, 118, 124, 110, 133, 137, 140, 153, 146, 142, 168, 141, 134, 147, 139, 142, 137, 138, 125, 116, 124, 121, 126, 123, 145, 135, 120, 136, 141, 140, 138, 130, 132, 144, 129, 121, 163, 129, 144, 129, 148, 142, 152, 128, 123, 133, 127, 122, 127, 149, 121, 131, 131, 124, 141, 133, 154, 127, 150, 142, 132, 180, 165, 162, 177, 154, 155, 135, 152, 156, 170, 175, 176, 161, 160, 157, 187, 219, 197, 191, 164, 192, 156, 178, 165, 169, 157, 183, 197, 184, 219, 220, 185, 232, 162, 203, 169, 193, 188, 204, 194, 168, 151, 179, 177, 181, 215, 208, 185, 161, 163, 169, 184, 178, 173, 155, 145, 177, 150, 170, 189, 156, 186, 172, 167, 169, 169, 159, 158, 149, 182, 175, 169, 176, 170, 150, 175, 189, 200, 192, 175, 164, 185, 200, 153, 181, 164, 160, 132, 141, 152, 166, 160, 145, 146, 204, 171, 161, 173, 199, 155, 182, 168, 173, 180, 152, 141, 140, 156, 155, 137, 169, 162, 137, 121, 155, 146, 137, 161, 157, 118, 139, 131, 150, 137, 128, 144, 142, 131, 143, 138, 144, 158, 144, 155, 151, 147, 112, 139, 132, 112, 136, 133, 116, 100, 119, 142, 136, 146, 134, 127, 166, 158, 150, 127, 141, 119, 109, 118, 158, 140, 135, 101, 118, 127, 132, 132, 105, 131, 108, 111, 113, 98, 113, 113, 123, 98, 106, 114, 115, 103, 97, 93, 101, 103, 92, 111, 97, 101, 111, 101, 84, 89, 117, 114, 110, 121, 128, 89, 102, 127, 99, 114, 115, 103, 110, 103, 103, 95, 101, 92, 98, 108, 81, 105, 87, 103, 94, 83, 99, 93, 80, 96, 88, 72, 109, 87, 83, 87, 94, 91, 104, 167, 173, 198, 153, 121, 102, 103, 102, 113, 135, 127, 100, 86, 144, 116, 119, 125, 104, 98, 91, 105, 91, 104, 101, 91, 108, 97, 120, 120, 117, 166, 121, 98, 116, 127, 112, 131, 124, 124, 165, 158, 115, 109, 147, 124, 112, 104, 97, 104, 97, 109, 121, 105, 146, 148, 176, 170, 169, 169, 165, 215, 187, 154, 185, 179, 137, 141, 139, 131, 130, 120, 123, 115, 138, 135, 131, 128, 112, 131, 138, 122, 114, 100, 115, 174, 165, 148, 151, 134, 131, 116, 127, 117, 148, 103, 137, 142, 110, 136, 122, 123, 134, 128, 102, 118, 132, 122, 135, 129, 147, 133, 117, 137, 157, 132, 146, 115, 114, 97, 108, 111, 105, 112, 124, 123, 122, 126, 131, 132, 129, 129, 155, 158, 215, 165, 134, 162, 154, 168, 174, 192, 244, 159, 246, 239, 293, 253, 200, 276, 234, 256, 220, 241, 180, 215, 191, 171, 176, 147, 171, 220, 236, 235, 223, 213, 224, 171, 174, 198, 178, 164, 190, 169, 162, 170, 162, 160, 173, 146, 152, 194, 162, 161, 155, 152, 140, 168, 155, 141, 154, 145, 191, 169, 164, 215, 153, 143, 158, 140, 186, 161, 151, 200, 169, 186, 175, 308, 267, 248, 196, 228, 261, 228, 227, 292, 163, 228, 224, 159, 229, 191, 199, 183, 172, 204, 151, 199, 195, 171, 137, 135, 158, 155, 145, 147, 145, 178, 120, 107, 138, 226, 172, 149, 179, 188, 160, 157, 108, 188, 148, 141, 139, 144, 135, 123, 144, 146, 131, 154, 145, 194, 163, 164, 170, 195, 177, 166, 175, 166, 178, 136, 148, 148, 141, 143, 155, 162, 161, 139, 167, 154, 150, 105, 130, 189, 156, 141, 151, 148, 158, 151, 164, 133, 139, 143, 136, 133, 154, 144, 126, 139, 123, 144, 143, 141, 110, 133, 112, 145, 122, 117, 104, 104, 135, 128, 111, 104, 144, 132, 152, 135, 151, 164, 141, 142, 150, 128, 134, 139, 148, 123, 129, 116, 115, 104, 124, 143, 130, 120, 115, 110, 104, 108, 103, 92, 100, 131, 109, 204, 171, 136, 170, 225, 145, 175, 255, 220, 193, 226, 136, 167, 146, 167, 157, 124, 116, 119, 128, 127, 115, 109, 123, 129, 120, 136, 168, 155, 128, 176, 132, 121, 141, 121, 149, 142, 179, 190, 167, 176, 195, 177, 180, 137, 112, 160, 150, 129, 97, 194, 131, 108, 83, 92, 76, 92, 80, 75, 97, 78, 81, 78, 86, 93, 133, 110, 112, 110, 104, 97, 107, 94, 126, 99, 104, 121, 124, 127, 107, 116, 111, 99, 83, 98, 136, 108, 107, 110, 114, 99, 90, 77, 111, 89, 93, 97, 92, 90, 81, 103, 100, 110, 99, 76, 90, 135, 120, 105, 91, 68, 84, 74, 89, 65, 78, 66, 62, 52, 57, 59, 63, 45, 44, 50, 54, 45, 50, 38, 50, 46, 39, 58, 40, 43, 41, 39, 39, 27, 37, 43, 29, 40, 35, 34, 30, 38, 28, 33, 31, 47, 36, 37, 36, 25, 31, 25, 28, 30, 26, 34, 29, 24, 33, 35, 21, 32, 19, 22, 12, 15, 19, 20, 27, 27, 22, 30, 14, 23, 22]
# array2 [33, 36, 51, 67, 47, 51, 42, 52, 63, 45, 60, 60, 43, 45, 43, 35, 37, 45, 41, 30, 50, 24, 38, 35, 34, 38, 41, 32, 29, 32, 32, 14, 32, 39, 30, 28, 29, 27, 34, 31, 40, 32, 24, 29, 32, 28, 26, 26, 27, 31, 36, 25, 33, 24, 27, 26, 15, 29, 26, 23, 28, 7, 24, 34, 27, 23, 25, 19, 24, 22, 20, 21, 19, 22, 11, 23, 28, 27, 24, 24, 24, 27, 22, 15, 20, 16, 17, 17, 28, 27, 14, 14, 15, 17, 22, 23, 24, 17, 21, 15, 17, 12, 14, 18, 10, 13, 9, 17, 13, 14, 12, 16, 10, 15, 12, 15, 14, 13, 10, 16, 13, 13, 15, 14, 14, 18, 14, 21, 28, 8, 9, 15, 13, 29, 6, 15, 21, 15, 17, 22, 16, 13, 18, 17, 15, 12, 16, 19, 15, 8, 19, 18, 16, 15, 18, 8, 12, 18, 12, 8, 12, 19, 14, 6, 13, 11, 9, 10, 21, 13, 11, 16, 12, 13, 14, 9, 7, 11, 8, 10, 11, 8, 7, 14, 8, 9, 16, 8, 8, 10, 6, 3, 12, 8, 11, 12, 13, 13, 12, 4, 10, 9, 11, 11, 8, 9, 20, 8, 7, 13, 19, 14, 14, 11, 12, 10, 7, 6, 10, 6, 6, 8, 6, 4, 8, 6, 13, 7, 6, 4, 6, 8, 11, 5, 2, 6, 5, 4, 5, 7, 12, 10, 15, 9, 7, 12, 16, 9, 8, 16, 12, 10, 11, 6, 5, 11, 8, 10, 7, 11, 12, 12, 9, 9, 8, 9, 6, 3, 7, 6, 8, 2, 3, 2, 1, 2, 2, 1, 4, 2, 1, 3, 4, 3, 4, 5, 3, 5, 7, 0, 3, 6, 2, 2, 0, 2, 4, 3, 1, 3, 5, 2, 4, 4, 2, 1, 2, 4, 5, 1, 2, 2, 0, 1, 2, 1, 4, 3, 4, 1, 4, 4, 4, 4, 4, 3, 3, 1, 4, 4, 4, 2, 6, 2, 2, 5, 1, 3, 4, 2, 6, 5, 3, 1, 2, 1, 2, 0, 1, 3, 5, 2, 3, 3, 6, 2, 2, 2, 4, 3, 0, 0, 2, 8, 3, 1, 10, 8, 1, 0, 3, 0, 5, 3, 3, 3, 5, 2, 3, 4, 6, 9, 7, 10, 7, 5, 3, 6, 6, 2, 5, 5, 1, 3, 10, 6, 3, 3, 2, 6, 5, 5, 6, 9, 5, 4, 7, 10, 10, 7, 5, 10, 5, 9, 7, 7, 3, 5, 4, 4, 16, 10, 12, 12, 7, 5, 9, 8, 9, 8, 14, 9, 5, 12, 8, 11, 11, 12, 6, 7, 12, 5, 10, 12, 16, 7, 13, 19, 5, 15, 9, 10, 13, 15, 14, 15, 14, 14, 13, 17, 16, 22, 21, 23, 20, 13, 13, 18, 29, 16, 18, 20, 26, 19, 27, 19, 24, 21, 28, 20, 26, 24, 17, 19, 23, 17, 15, 20, 29, 33, 39, 25, 28, 25, 36, 41, 29, 35, 35, 39, 29, 38, 45, 34, 37, 48, 40, 33, 38, 47, 47, 29, 41, 49, 44, 47, 49, 35, 48, 43, 55, 42, 52, 48, 52, 47, 38, 51, 54, 52, 55, 45, 44, 73, 53, 70, 67, 71, 66, 58, 57, 76, 55, 84, 60, 76, 58, 70, 78, 73, 66, 65, 70, 62, 73, 60, 66, 79, 61, 65, 74, 70, 93, 78, 53, 81, 69, 78, 75, 73, 88, 92, 63, 86, 72, 90, 68, 82, 106, 94, 87, 77, 87, 82, 95, 92, 95, 82, 90, 93, 103, 88, 81, 93, 75, 101, 93, 100, 90, 91, 86, 83, 87, 117, 107, 104, 109, 72, 89, 106, 100, 93, 110, 71, 97, 87, 117, 106, 86, 110, 94, 102, 119, 123, 101, 113, 107, 122, 107, 112, 111, 117, 118, 107, 115, 105, 130, 118, 129, 114, 108, 96, 121, 118, 117, 110, 137, 116, 119, 114, 127, 123, 127, 110, 119, 113, 133, 130, 132, 119, 142, 121, 125, 140, 119, 145, 137, 119, 138, 126, 114, 127, 127, 133, 131, 148, 138, 135, 115, 113, 139, 146, 119, 144, 136, 140, 146, 151, 130, 130, 135, 131, 156, 155, 128, 134, 139, 152, 132, 157, 136, 166, 175, 158, 143, 155, 110, 134, 128, 146, 139, 169, 136, 149, 159, 158, 154, 134, 155, 176, 145, 162, 157, 145, 134, 160, 164, 178, 171, 139, 148, 161, 147, 157, 165, 160, 144, 149, 131, 160, 157, 133, 184, 153, 184, 143, 123, 177, 157, 139, 163, 154, 171, 162, 171, 154, 165, 144, 169, 144, 179, 160, 176, 173, 177, 168, 174, 162, 149, 133, 151, 150, 174, 158, 168, 162, 145, 154, 185, 162, 197, 177, 176, 177, 153, 166, 171, 177, 149, 192, 170, 146, 173, 157, 172, 135, 161, 173, 169, 158, 145, 151, 168, 151, 162, 158, 155, 168, 176, 166, 162, 141, 181, 176, 168, 160, 185, 155, 173, 151, 182, 179, 149, 161, 148, 149, 140, 164, 140, 152, 156, 150, 161, 148, 138, 157, 151, 157, 133, 157, 143, 155, 173, 135, 158, 147, 154, 149, 164, 157, 146, 143, 131, 150, 140, 140, 124, 132, 123, 179, 128, 137, 136, 123, 140, 166, 151, 133, 136, 154, 143, 131, 137, 152, 156, 151, 150, 127, 133, 146, 147, 135, 132, 135, 152, 116, 110, 148, 140, 140, 121, 119, 150, 129, 136, 123, 144, 151, 137, 135, 129, 125, 130, 123, 132, 133, 130, 123, 129, 128, 115, 174, 252, 225, 242, 151, 158, 134, 157, 146, 158, 179, 145, 112, 145, 157, 154, 152, 141, 120, 113, 121, 106, 119, 115, 109, 118, 142, 117, 147, 143, 161, 178, 151, 114, 149, 141, 132, 143, 132, 123, 170, 147, 171, 126, 125, 147, 137, 114, 122, 118, 130, 134, 196, 155, 226, 210, 209, 247, 215, 207, 195, 224, 224, 172, 197, 202, 153, 158, 156, 135, 155, 129, 126, 120, 151, 127, 132, 134, 125, 139, 141, 152, 106, 115, 124, 140, 152, 154, 128, 134, 139, 121, 137, 139, 132, 114, 144, 141, 131, 112, 116, 126, 123, 115, 102, 115, 123, 112, 121, 135, 118, 137, 106, 121, 166, 141, 123, 110, 112, 103, 106, 100, 108, 111, 106, 119, 106, 105, 113, 112, 130, 125, 164, 194, 159, 155, 134, 165, 181, 192, 183, 257, 276, 238, 240, 197, 262, 230, 197, 235, 199, 219, 187, 216, 182, 188, 164, 178, 155, 149, 139, 186, 195, 170, 176, 173, 172, 156, 145, 185, 148, 149, 153, 168, 136, 160, 152, 148, 125, 136, 135, 163, 134, 153, 154, 142, 153, 118, 156, 120, 131, 139, 152, 122, 163, 162, 130, 141, 159, 134, 181, 171, 149, 286, 179, 199, 210, 358, 315, 325, 268, 251, 244, 228, 232, 274, 190, 252, 232, 176, 234, 232, 228, 190, 189, 178, 163, 171, 180, 160, 160, 139, 159, 200, 148, 167, 183, 192, 152, 118, 119, 138, 181, 161, 185, 166, 136, 139, 184, 171, 138, 130, 133, 123, 135, 140, 134, 143, 148, 147, 117, 145, 160, 147, 161, 166, 159, 188, 145, 151, 138, 163, 139, 132, 124, 119, 153, 154, 147, 130, 139, 128, 122, 118, 132, 139, 138, 125, 133, 118, 161, 138, 176, 151, 161, 124, 134, 145, 142, 150, 146, 135, 129, 136, 139, 141, 141, 157, 134, 135, 132, 139, 143, 129, 125, 158, 104, 128, 131, 115, 153, 164, 126, 138, 126, 140, 143, 139, 127, 147, 160, 142, 135, 150, 107, 129, 122, 148, 139, 115, 132, 123, 142, 134, 130, 135, 131, 126, 182, 269, 158, 166, 172, 279, 449, 222, 214, 206, 181, 160, 162, 164, 171, 276, 211, 174, 151, 173, 223, 181, 166, 175, 166, 193, 173, 184, 203, 185, 158, 203, 171, 149, 150, 164, 195, 165, 180, 184, 187, 161, 215, 182, 194, 158, 221, 140, 136, 119, 199, 158, 100, 119, 92, 100, 93, 93, 69, 97, 81, 96, 90, 96, 80, 100, 127, 115, 111, 124, 134, 120, 114, 124, 109, 125, 125, 132, 133, 137, 123, 122, 122, 110, 93, 105, 147, 117, 127, 125, 121, 117, 112, 101, 115, 111, 105, 111, 118, 106, 93, 132, 112, 125, 120, 90, 126, 142, 128, 116, 97, 89, 92, 98, 86, 78, 90, 84, 76, 83, 72, 79, 86, 72, 70, 54, 62, 51, 64, 69, 57, 61, 52, 70, 56, 54, 60, 68, 49, 54, 68, 62, 57, 58, 51, 54, 52, 55, 46, 53, 54, 56, 63, 47, 62, 49, 48, 61, 49, 53, 45, 41, 41, 46, 45, 58, 49, 58, 47, 43, 37, 36, 42, 49, 49, 45, 45, 39, 36, 51, 61]


conn.close()