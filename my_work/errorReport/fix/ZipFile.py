import os,os.path
import zipfile
import time
import sys
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')


if os.path.exists(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady.txt'):
    os.remove(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady.txt')
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
                print 'root',root, dirs, files
                for name in files:
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
    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    zip_dir(r'/tmp/ErrorReportPro/errorReport/report',r'/tmp/ErrorReportPro/errorReport/ERROR REPORT_%s.zip'%otherStyleTime)


f = open(r'/tmp/ErrorReportPro/errorReport/ZipFileIsReady.txt', 'w')
f.close()