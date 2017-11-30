#coding:UTF-8
import pydoop.hdfs
import pydoop.hdfs as hdfs
from_path = '/tmp/cctv/abc.txt'
to_path = 'hdfs://localhost:22/tmp/outfile.txt'
hdfs.put(from_path,to_path)