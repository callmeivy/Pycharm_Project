'''
__author__ = 'Ivy'
'''
# coding=UTF-8
import sys
list1=[1,2,3]
list2=[3,4,5]
from numpy import loads, mean, sum, nan
# s = '%d rows.' % len(list1)
# print s
# if 1 == 1:
#     print s
#     # s += '\nE.g: %s' % str(list1[0])
#     s = s + '\nE.g: %s' % str(list1[0])
#     print 's',s

list1.extend(list2)
print list1
# 1,2,3=tuple
# if 1 == 1:
# raise ValueError('Tuple format not correct (should be: <value, row_id, col_id>)')

# value, row_id, col_id = tuple

path = 'abd'


sys.stdout.write('Loading %s\n' % path)
print 'Loading %s\n' % path

# value = unicode('china', 'utf8')
# print value

s = '\t'.join(['1','2','3'])
print s

# dump
list1.pop(4)
print 'list1',list1


print "Sumj_Sk"