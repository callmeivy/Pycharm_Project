#coding:UTF-8
#encoding:UTF-8
import pyhdfs
import sys
for pth in sys.path:
    print pth
from hdfs.hfile import Hfile

hostname = '192.168.168.162'
port = 8020
hdfs_path = '/examples/hdfs'
local_path = 'E:\NewData\VSP-outlet-1_tomcat_20150317.log'

hfile = Hfile(hostname, port, hdfs_path, mode='w')
fh = open(local_path)

for line in fh:
    hfile.write(line)

fh.close()
hfile.close()

