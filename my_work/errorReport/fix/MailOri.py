#coding:UTF-8
'''
Created on 22 Oct 2014

class for send mail with attach

@author: Administrator
'''
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
reload(sys)
sys.setdefaultencoding('utf8')

sys.path.append('..')
sys.path.append('/tmp/ErrorReportPro/errorReport/')

class Mailing():
    def __init__(self, mailList, localhost, localuser, localpass, localprefix, encodingFormat = 'utf-8'):
        '''
        #设置发送邮件名单,服务器,用户名,口令以及邮箱的后缀
        e.g.
        mailList = ["xxx@ctvit.com.cn"] 
        localhost = "smtp.ctvit.com.cn"
        localuser = "xxx@ctvit.com.cn"
        localpass = "xxx"
        localprefix="ctvit.com.cn"
        
        有缺省值
        encodingFormat = 'gbk'
        '''
        self.mailList = mailList
        self.localhost = localhost
        self.localuser = localuser
        self.localpass = localpass
        self.localprefix = localprefix
        
        self.encodingFormat = encodingFormat
    
    def __del__(self):
        pass
    
    def sendWithoutAttach(self, sub, content):
        '''
        无附件邮件
        '''
        me=self.localuser+"<"+self.localuser+"@"+self.localprefix+">"
        msg = MIMEText(content,_charset=self.encodingFormat)
        # msg = MIMEText(content,'text','utf-8')
        msg['Subject'] = sub.encode('utf-8')
        msg['From'] = me
        msg['To'] = ";".join(self.mailList)
        try:
            s = smtplib.SMTP() 
            s.connect(self.localhost)
            # s.login(self.localuser,self.localpass)
            s.sendmail(me, self.mailList, msg.as_string()) 
            s.quit() 
            s.close()
            return 1
        except Exception, e:  
            print str(e)  
            return 0 
    
    def sendWithAttach(self, attachPath1, rename, sub, content):
        '''
        创建一个带附件的实例
        '''
        msg = MIMEMultipart()
        
        #构造附件1
        att1 = MIMEText(open(attachPath1, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = 'attachment; filename=%s' %rename.encode('utf-8')#这里的filename可以任意写，写什么名字，邮件中显示什么名字
        msg.attach(att1)
        
        part1 = MIMEText(content.encode(), 'plain',_charset='utf-8' )
        msg.attach(part1)
        
        #加邮件头
        me=self.localuser+"<"+self.localuser+"@"+self.localprefix+">"
        msg['Subject'] = sub.encode('utf-8')
        msg['From'] = me
        msg['To'] = ";".join(self.mailList)
        
        
        #发送邮件
        try:
            s = smtplib.SMTP() 
            s.connect(self.localhost)
            # s.login(self.localuser,self.localpass)
            s.sendmail(me, self.mailList, msg.as_string()) 
            s.quit()
            s.close()
            return 1
        except Exception, e:  
            print str(e)  
            return 0 
        


if __name__=='__main__':
    a=Mailing(mailList = ['sunjian@ctvit.com.cn','wangyue1@bgctv.com.cn','tiansichang@bgctv.com.cn', 'qifeng@bgctv.com.cn ', 'yuanhao@ctvit.com.cn',"jincan@ctvit.com.cn"], localhost = "172.16.251.72", localuser = "",
              localpass = "", localprefix="")
    # a=Mailing(mailList = ['wangyue1@bgctv.com.cn',"jincan@ctvit.com.cn"], localhost = "172.16.251.72", localuser = "jincan@ctvit.com.cn",
    #           localpass = "!Joyce87", localprefix="")
    # a=Mailing(mailList = ['wangyue1@bgctv.com.cn','jincan@ctvit.com.cn'], localhost = "172.16.251.72", localuser = "jincan@ctvit.com.cn",localpass = "!Joyce87", localprefix="")
    # a=Mailing(mailList = ["jincan@ctvit.com.cn"], localhost = "172.16.251.72", localuser = "",
    #           localpass = "", localprefix="")


    # ret = a.sendWithoutAttach('共享文件夹', '共享文件夹')
    #
    # if ret>=1:
    #     print "发送成功"
    # else:
    #     print "发送失败"

    # ret = a.sendWithAttach("C:/Users/Ivy/Desktop/d.py", 'ultraEdit.zip', '共享文件夹', '请查收')
    # if ret>=1:
    #     print "发送成功"
    # else:
    #     print "发送失败"

    now = int(time.time())
    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    print otherStyleTime
    fileName=r'/tmp/ErrorReportPro/errorReport/ERROR REPORT_%s.zip'%otherStyleTime
    # fileName=r'/ERROR REPORT_%s.zip'%otherStyleTime
    print fileName
    #文件名不能有空格
    name_it='ERROR_REPORT_'+otherStyleTime+'.zip'
# ultraEdit
    ret = a.sendWithAttach(fileName, str(name_it), '点播错误报表统计_'+otherStyleTime, '点播错误报表统计_'+otherStyleTime+',请查收。')
    if ret>=1:
        print "succeed!"
    else:
        print "FAILED!"
    

