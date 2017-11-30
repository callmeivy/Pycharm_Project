# coding=UTF-8

import pydoop.hdfs as hdfs
with hdfs.open('/tmp/data/cctv/comment.txt') as f:
    for line in f:
        print(line)