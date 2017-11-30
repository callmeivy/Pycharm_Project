#coding:UTF-8
import sys
import os
import os.path
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')

# os.system(r'/tmp/ErrorReportPro/errorReport/addColumnTimeAndPeriod.py')
os.system('python /tmp/ErrorReportPro/errorReport/addColumnTimeAndPeriodUseIt.py')
# execfile(r'/tmp/ErrorReportPro/errorReport/addColumnTimeAndPeriod.py')




os.system('python /tmp/ErrorReportPro/errorReport/GH-CDN-A-NGid5225.py')
if os.path.exists('/tmp/ErrorReportPro/errorReport/5225IsReady.txt'):
    message = '5.OK, GH-CDN-A-NGid5225 is successfully executed.'
    print message


    os.system('python /tmp/ErrorReportPro/errorReport/VODSum.py')
    if os.path.exists('/tmp/ErrorReportPro/errorReport/VODSumIsReady.txt'):
        message = '6.OK, VODSum is successfully executed.'
        print message

        os.system('python /tmp/ErrorReportPro/errorReport/ErrorSum.py')
        if os.path.exists('/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'):
            message = '7.OK,ErrorSum is successfully executed.'
            print message

            os.system('python /tmp/ErrorReportPro/errorReport/ServiceTimeOut.py')
            if os.path.exists('/tmp/ErrorReportPro/errorReport/ServiceTimeOutIsReady.txt'):
                message = '8.OK,ServiceTimeOut is successfully executed.'
                print message

                os.system('python /tmp/ErrorReportPro/errorReport/ZipFile.py')
                if os.path.exists('/tmp/ErrorReportPro/errorReport/ZipFileIsReady.txt'):
                    message = '9.OK,ZipFile is successfully executed.'
                    print message


                    try:
                        os.system('python /tmp/ErrorReportPro/errorReport/Mailtest.py')
                        message = '10.OK,all files are successfully executed.'
                        print message
                    except:
                        message = 'oops!Mail encounter a problem!'
                        print message

                else:
                    message = 'oops!ZipFile encounter a problem!'
                    print message

            else:
                message = 'oops!ServiceTimeOut encounter a problem!'
                print message


        else:
            message = 'oops!ErrorSum encounter a problem!'
            print message




    else:
        message = 'oops!VODSum encounter a problem!'
        print message




else:
    message = 'oops!GH-CDN-A-NGid5225 encounter a problem!'
    print message






#
#     filename = r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'
#     # filename = r'C:\Users\Ivy\PycharmProjects\errorReport\1.py'
#     if os.path.exists(filename):
#
#         message = 'OK, the "%s" file exists.'
#         print message % filename