#coding:UTF-8
import os,os.path
import zipfile
import time
import sys
import re
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')


if os.path.exists(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady2.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady2.txt')

excludes = ['CDN_B','CDN_E','CDN_H','CDN_D']

ex_file = ['ServiceTimeOut.xls']
def zip_dir(dirname,zipfilename):
    filelist = []
    filename = r'/tmp/ErrorReportPro/errorReport/GraphIsReady.txt'
    # filename = r'C:\Users\Ivy\PycharmProjects\errorReport\1.py'
    if os.path.exists(filename):

        message = 'OK, the "%s" file exists.'
        print message % filename


        if os.path.isfile(dirname):
            print 'dirname',dirname
            filelist.append(dirname)

        else :
            for root, dirs, files in os.walk(dirname):
                print 'root',root,'dirs', dirs, "files",files
                dirs[:] = [d for d in dirs if d not in excludes]
                print "change dirs",dirs[:]
                for name in files:
                    if name not in ex_file:
                        print 'name',name
                        filelist.append(os.path.join(root, name))

        zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
        print 333


        for tar in filelist:
            print 444
            arcname = tar[len(dirname):]
            print arcname
            zf.write(tar,arcname)
        zf.close()
    else:
        message = 'Sorry, I cannot find the "%s" file.'
    print message % filename





if __name__ == '__main__':
    # 如果要跑昨天的代码，改这里%%%%%%%%%%%%%%%%%%%%%%%%%
    now = int(time.time())-86400*9
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    zip_dir(r'/tmp/ErrorReportPro/errorReport/report',r'/tmp/ErrorReportPro/errorReport/ERROR REPORT2_%s.zip'%otherStyleTime)


f = open(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady2.txt', 'w')
f.close()