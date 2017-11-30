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
iae_audiencerate_new=conn.gehua.iae_audiencerate_new
array = [0] * 1440
array2 = [0] * 1440
caid_can=list()
caid_time=defaultdict(list)
caid_time2=defaultdict(list)
caid_box=list()
# array [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, Fraction(431, 60)(Index:1265), Fraction(779, 20), Fraction(4411, 60), Fraction(1579, 15), Fraction(4753, 30), Fraction(668, 3), Fraction(1543, 6), Fraction(17279, 60), Fraction(18829, 60), Fraction(3147, 10), Fraction(3111, 10), Fraction(18773, 60), Fraction(3169, 10), Fraction(637, 2), Fraction(1013, 3), Fraction(5318, 15), Fraction(751, 2), Fraction(11429, 30), Fraction(3277, 10), Fraction(9611, 30), Fraction(9257, 30), Fraction(5971, 20), Fraction(18377, 60), Fraction(1617, 5), Fraction(7091, 20), Fraction(4633, 12), Fraction(1621, 4), Fraction(6467, 15), Fraction(8571, 20), Fraction(1345, 3), Fraction(13537, 30), Fraction(450, 1), Fraction(14473, 30), Fraction(9913, 20), Fraction(2562, 5), Fraction(15229, 30), Fraction(6209, 12), Fraction(1565, 3), Fraction(15767, 30), Fraction(10749, 20), Fraction(8182, 15), Fraction(8156, 15), Fraction(3349, 6), Fraction(8398, 15), Fraction(2249, 4), Fraction(2804, 5), Fraction(6697, 12), Fraction(27971, 60), Fraction(12863, 30), Fraction(2393, 5), Fraction(9929, 20), Fraction(9969, 20), Fraction(15361, 30), Fraction(6329, 12), Fraction(8663, 15), Fraction(37001, 60), Fraction(1273, 2), Fraction(9547, 15), Fraction(9356, 15), Fraction(12903, 20), Fraction(3907, 6), Fraction(13197, 20), Fraction(6587, 10), Fraction(19243, 30), Fraction(20039, 30), Fraction(20527, 30), Fraction(6987, 10), Fraction(13849, 20), Fraction(8087, 12), Fraction(10298, 15), Fraction(2045, 3), Fraction(2075, 3), Fraction(41239, 60), Fraction(40291, 60), Fraction(10342, 15), Fraction(42049, 60), Fraction(4241, 6), Fraction(10601, 15), Fraction(41057, 60), Fraction(21199, 30), Fraction(21407, 30), Fraction(43387, 60), Fraction(2183, 3), Fraction(42437, 60), Fraction(43457, 60), Fraction(10864, 15), Fraction(3651, 5), Fraction(43943, 60), Fraction(42673, 60), Fraction(45989, 60), Fraction(3125, 4), Fraction(11944, 15), Fraction(23929, 30), Fraction(46421, 60), Fraction(3929, 5), Fraction(7811, 10), Fraction(7573, 10), Fraction(11339, 15), Fraction(14549, 20), Fraction(3977, 6), Fraction(8806, 15), Fraction(34931, 60), Fraction(8726, 15), Fraction(33341, 60), Fraction(34877, 60), Fraction(19063, 30), Fraction(40859, 60), Fraction(10373, 15), Fraction(8191, 12), Fraction(14041, 20), Fraction(1373, 2), Fraction(1796, 3), Fraction(8837, 15), Fraction(17309, 30), Fraction(17507, 30), Fraction(34561, 60), Fraction(36497, 60), Fraction(3013, 5), Fraction(17033, 30), Fraction(25459, 60), Fraction(13523, 60), Fraction(1697, 12), Fraction(673, 10), Fraction(1423, 60), Fraction(371, 30), Fraction(9, 1), Fraction(9, 2), 1, 1, 0, 0, 0, 0, 0, 0, Fraction(47, 4), Fraction(1381, 20), Fraction(2293, 20), Fraction(2023, 12), Fraction(14473, 60), Fraction(15713, 60), Fraction(5579, 20), Fraction(4304, 15), Fraction(1437, 5), Fraction(17581, 60), Fraction(1693, 6), Fraction(17393, 60), Fraction(8731, 30), Fraction(8509, 30), Fraction(3445, 12), Fraction(3301, 12), Fraction(3692, 15), Fraction(3338, 15), Fraction(2659, 12), Fraction(13133, 60), Fraction(12929, 60), Fraction(4329, 20), Fraction(2635, 12), Fraction(6869, 30), Fraction(2881, 12), Fraction(963, 4), Fraction(1433, 6), Fraction(4479, 20), Fraction(11639, 60), Fraction(8731, 60), Fraction(6391, 60), Fraction(1177, 15), Fraction(3731, 60), Fraction(779, 10), Fraction(6593, 60), Fraction(7631, 60), Fraction(3439, 30), Fraction(2971, 30), Fraction(355, 6), Fraction(221, 20)]

# ！！！！！！！！！！！！！！！！！！！！！！！！！！！这是不重复观众数！！！！！！！！！！！！！！！！！
# lines=eval("iae_audiencerate_new.find({'WIC.date':'%s','WIC.A.sn':'%s'}).batch_size(30)" %('2014-12-04','浙江卫视'))
lines=eval("iae_audiencerate_new.find({'WIC.date':'%s'}).batch_size(30)" %('2014-12-04'))
for line in lines:
    print "111",line
    L=len(line['WIC']['A'])
    if type(line['WIC']['A']) is list:
        for s in range(0,L):
            if line['WIC']['A'][s]['sn'].encode('utf-8')=='浙江卫视':
                starttime=line['WIC']['A'][s]['s']
                caid=line['WIC']['cardNum']
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

    else:
        if line['WIC']['A']['sn'].encode('utf-8')=='浙江卫视':
            caid=line['WIC']['cardNum']
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

conn.close()