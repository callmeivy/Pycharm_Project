#coding:UTF-8
import sys
import os
import os.path
sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')

# os.system(r'/tmp/ErrorReportPro/errorReport/addColumnTimeAndPeriod.py')
os.system('python /tmp/ErrorReportPro/errorReport/fix5days/addColumnTimeAndPeriodUseItv.py')
# execfile(r'/tmp/ErrorReportPro/errorReport/addColumnTimeAndPeriod.py')


if os.path.exists('/tmp/ErrorReportPro/errorReport/NewDataUpdatingIsReady.txt'):
    message = '1.OK, addColumnTimeAndPeriod is successfully executed.'
    print message
    os.system('python /tmp/ErrorReportPro/errorReport/fix5days/GH-CDN-A-NGid01300v.py')
    if os.path.exists('/tmp/ErrorReportPro/errorReport/01-300IsReady.txt'):
        message = '2.OK, GH-CDN-A-NGid01300 is successfully executed.'
        print message

        os.system('python /tmp/ErrorReportPro/errorReport/fix5days/GH-CDN-A-NGid02300v.py')
        if os.path.exists('/tmp/ErrorReportPro/errorReport/02-300IsReady.txt'):
            message = '3.OK, GH-CDN-A-NGid02300 is successfully executed.'
            print message

            os.system('python /tmp/ErrorReportPro/errorReport/fix5days/GH-CDN-A-NGid5206v.py')
            if os.path.exists('/tmp/ErrorReportPro/errorReport/5206IsReady.txt'):
                message = '4.OK, GH-CDN-A-NGid5206 is successfully executed.'
                print message

                os.system('python /tmp/ErrorReportPro/errorReport/fix5days/GH-CDN-A-NGid5225v.py')
                if os.path.exists('/tmp/ErrorReportPro/errorReport/5225IsReady.txt'):
                    message = '5.OK, GH-CDN-A-NGid5225 is successfully executed.'
                    print message


                    os.system('python /tmp/ErrorReportPro/errorReport/fix5days/VODSumv.py')
                    if os.path.exists('/tmp/ErrorReportPro/errorReport/VODSumIsReady.txt'):
                        message = '6.OK, VODSum is successfully executed.'
                        print message

                        os.system('python /tmp/ErrorReportPro/errorReport/fix5days/ErrorSumv.py')
                        if os.path.exists('/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'):
                            message = '7.OK,ErrorSum is successfully executed.'
                            print message

                            os.system('python /tmp/ErrorReportPro/errorReport/fix5days/FixOneDayServiceTimeOut.py')
                            if os.path.exists('/tmp/ErrorReportPro/errorReport/ServiceTimeOutIsReady.txt'):
                                message = '8.OK,ServiceTimeOut is successfully executed.'
                                print message

                                os.system('python /tmp/ErrorReportPro/errorReport/fix5days/graphv.py')
                                if os.path.exists('/tmp/ErrorReportPro/errorReport/GraphIsReady.txt'):
                                    message = '9.OK,ServiceTimeOut is successfully executed.'
                                    print message


                                    os.system('python /tmp/ErrorReportPro/errorReport/fix5days/Fix3DayZipFileSplit1.py')
                                    if os.path.exists('/tmp/ErrorReportPro/errorReport/ZipFileIsReady1.txt'):
                                        message = '10.OK,ZipFile1 is successfully executed.'
                                        print message

                                        os.system('python /tmp/ErrorReportPro/errorReport/fix5days/Fix3DayZipFileSplit1.py')
                                        if os.path.exists('/tmp/ErrorReportPro/errorReport/ZipFileIsReady2.txt'):
                                            message = '11.OK,ZipFile2 is successfully executed.'
                                            print message

                                            try:
                                                os.system('python /tmp/ErrorReportPro/errorReport/fix5days/MailTwoFiles1.py')
                                                message = '12.OK,all files are successfully executed.'
                                                print message
                                            except:
                                                message = 'oops!Mail1 encounter a problem!'
                                                print message

                                            try:
                                                os.system('python /tmp/ErrorReportPro/errorReport/fix5days/MailTwoFiles2.py')
                                                message = '13.OK,all files are successfully executed.'
                                                print message
                                            except:
                                                message = 'oops!Mail2 encounter a problem!'
                                                print message


                                        else:
                                            message = 'oops!ZipFile2 encounter a problem!'
                                            print message

                                    else:
                                        message = 'oops!ZipFile1 encounter a problem!'
                                        print message


                                else:
                                    message = 'oops!graph encounter a problem!'
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




            else:
                message = 'oops!GH-CDN-A-NGid5206 encounter a problem!'
                print message




        else:
            message = 'oops!GH-CDN-A-NGid02300 encounter a problem!'
            print message

    else:
        message = 'oops!GH-CDN-A-NGid01300 encounter a problem!'
        print message

else:
    message = 'oops!addColumnTimeAndPeriod encounter a problem!'
    print message

#
#     filename = r'/tmp/ErrorReportPro/errorReport/ErrorSumIsReady.txt'
#     # filename = r'C:\Users\Ivy\PycharmProjects\errorReport\1.py'
#     if os.path.exists(filename):
#
#         message = 'OK, the "%s" file exists.'
#         print message % filename