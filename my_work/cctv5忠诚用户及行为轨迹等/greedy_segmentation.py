#coding:UTF-8
'''
贪婪分割法发现周期模式

Created on 2017年1月17日

@author: Ivy

'''
import numpy as np

#/ 方差阈值
alpha = 0.02
#/ 最大周期差异比例阈值
gama = 0.5
# 最小开机时间段比例
beta = 0.5
node = list()
#/ 是否含子序列
children = False
yes_list = list()

#/ 分裂阶段

#/ 方差比 Ve/Vr, Ve是当前序列的方差，Vr=T^2/N^2,T是起始时间与最后时间时间差，N为时间点
def variance_ratio(lis_interval):
    node_variance = np.var(lis_interval)
    #/ T:all_time_differ
    all_time_differ = sum(lis_interval)
    # N: number
    number = len(lis_interval)+1
    time_win_variance = np.square(round(float(float((all_time_differ) / float((number)))),3))
    # print 'kkkk', time_win_variance
    variance_ratio = float(float(node_variance) / float(time_win_variance))
    # print 'ff', variance_ratio
    return variance_ratio




time_interval_split = list()
# def split_seqence(lis):
ratio_list = list()
#/ lis是时间间隔序列，不是点序列，寻找分裂点，将序列分裂成两个序列
# def split_seqence(lis):
#     for slice_index in range(1,len(lis)-1):
#     #     print 'slice', slice_index, time_interval_inital[0:slice_index],\
#     # time_interval_inital[(slice_index+1):len(time_interval_inital)]
#         time_interval_left = lis[0:slice_index]
#         # print 'left',  time_interval_left
#         time_interval_right = lis[(slice_index + 1):len(lis)]
#         # print 'right',  time_interval_right
#         value_sum_ratio = variance_ratio(time_interval_left) + variance_ratio(time_interval_right)
#         ratio_list.append(value_sum_ratio)
#     # print 'kk', ratio_list
#     min_ratio = min(ratio_list)
#     print min_ratio, ratio_list.index(min_ratio)
#     print "分裂点index", ratio_list.index(min_ratio)
#     split_index = ratio_list.index(min_ratio) + 1
#     time_interval_split.append(lis[0:split_index])
#     time_interval_split.append(lis[split_index+1:len(lis)])
#     print "左右序列", lis[0:split_index], lis[split_index+1:len(lis)]
#     return time_interval_split

#/ lis是点序列，不是间隔序列
def split_seqence(lis_ori):
    lis_inter = turn_into_interval(lis_ori)[1]
    # print "lis_inter", lis_inter
    ratio_list = []
    time_interval_split = []
    for slice_index in range(2,len(lis_inter)-1):
    #     print 'slice', slice_index, time_interval_inital[0:slice_index],\
    # time_interval_inital[(slice_index+1):len(time_interval_inital)]
        time_interval_left = lis_inter[0:slice_index]
        # print 'left',  time_interval_left
        time_interval_right = lis_inter[(slice_index ):len(lis_inter)]
        # print 'right',  time_interval_right
        value_sum_ratio = variance_ratio(time_interval_left) + variance_ratio(time_interval_right)
        ratio_list.append(value_sum_ratio)
    # print 'kk', ratio_list
    min_ratio = min(ratio_list)
    print min_ratio, ratio_list.index(min_ratio)
    split_index = ratio_list.index(min_ratio) + 1
    # print "时间点序列分裂点index", split_index+1
    split_left = lis_ori[0:split_index+2]
    split_right = lis_ori[split_index+1:len(lis_ori)]
    time_interval_split.append(split_left)
    time_interval_split.append(split_right)
    # print "左右序列", split_left, split_right
    return time_interval_split

def turn_into_interval(lis):
    time_interval = []
    for i in range(len(lis)-1):
        # print 'i', i
        j = lis[i+1]-lis[i]
        # print j
        time_interval.append(j)
    return lis, time_interval




# time_spot = [1, 3, 5, 10, 13, 17, 21,25]
time_spot = [1, 3, 5, 7,9, 13, 17, 21,25]
# time_interval_inital = [2,2,5,3,4,4,4]
time_interval_inital = turn_into_interval(time_spot)[1]


#/ 原始序列是否满足方差比小于阈值，如果是，不用pop
# print "方差比", variance_ratio(time_interval_inital)
# if variance_ratio(time_interval_inital) <= alpha and len(time_interval_inital) >= 1:
#     yes_list.append(time_interval_inital)
#
# else:
    # # 从末尾一个一个pop，每次都判断方差比
    # while len(time_interval_inital) > 1:
        # pop_element = time_interval_inital.pop()
        # / node序列的方差

#/ yes_list装的是时间点序列
yes_list.append(time_spot)
kaiguan = 0
result = []
while len(yes_list) > 0:
    kaiguan += 1
    if kaiguan > 5:
        break
    node = yes_list.pop()
    node_inter = turn_into_interval(node)[1]
    # print '333', node, node_inter
    # print '111', yes_list
    if variance_ratio(node_inter) <= alpha and len(node_inter) >= 3:
        children = False
        # print '符合', node, node_inter
        result.append(node)
        # yes_list.append(node)
    #/ 寻找分裂点
    elif len(node_inter) >= 3:
        # print 'wwwfff', split_seqence(node)
        yes_list.extend(split_seqence(node))
        print 'fenlie', yes_list
print 'result', result


# / 周期类型判断
whole_len = len(time_spot)
child_len_sum = 0
for child_list in result:
    child_len_sum = child_len_sum + len(child_list)
# m_max是最大周期
m_max = max(turn_into_interval(child_list)[1])
m_min = min(turn_into_interval(child_list)[1])
if (m_max - m_min)/m_min <= gama:
    print "whole_len", whole_len, "child_len_sum", child_len_sum
    if whole_len == child_len_sum:
        #/ 等周期的周期模式
        type = 0
    elif child_len_sum/whole_len >= beta:
        # / 等周期的部分周期模式
        type = 1
else:
    if whole_len == child_len_sum:
        #/ 周期变化的周期模式
        type = 2
    elif child_len_sum/whole_len >= beta:
        #/ 周期变化的部分周期模式
        type = 3
print  type