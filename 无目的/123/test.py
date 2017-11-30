#coding:UTF-8
'''
Created on 2016年3月2日

@author: Ivy
观众口碑，抓取微博用户评论数据，进行情感分析
结果对应sql表是customer_evaluation
3月24日补充，
以“JQJM”为关键词的微博原贴代替，按理应该是底下的评论
3月25日补充，
其他竞争栏目的观众口碑也是从这里出
4月5日补充，
加上日期这一维度
'''
import sys,os
from sys import path
path.append('tools/')
path.append(path[0]+'/tools')
import MySQLdb
# from spammerDetection import spammerdetect
import jieba
import time

reload(sys)
sys.setdefaultencoding('utf8')

def addcustomerEvaluation_informal(hbaseIP,mysqlhostIP, mysqlUserName = 'root', mysqlPassword = '', dbname = 'btv'):


    
    # 连接数据库
    sqlConn=MySQLdb.connect(host=mysqlhostIP, user=mysqlUserName, passwd=mysqlPassword, db = dbname, charset='utf8')
    sqlcursor = sqlConn.cursor()

    sqlcursor.execute('''CREATE TABLE IF NOT EXISTS customer_evaluation(pk bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, flag int(1), evaluation bigint(20), content varchar(200), date Date,
                    program_id varchar(200), program varchar(200)) DEFAULT CHARSET=utf8;''')
    print '新建库成功'
    

    # 首先获取栏目
    sqlcursor.execute('''SELECT DISTINCT(program) from competition_analysis''')
    bufferTemp = sqlcursor.fetchall()
    print type(list(bufferTemp))
    for one_program in bufferTemp:
        one_program = one_program[0]
        print one_program

    # # 以“JQJM”为关键词的微博原贴代替，按理应该是底下的评论
    # table=conn.table('WEIBO_POST_JQJM')
    # # customerEvaluation_informal需要有program标识
    # emotionsWord = []
    # emotionsScore = 0
    # count = 0
    # printCount = 0
    #
    # # 时间属性
    # # inter为0，即为当日
    # inter = 0
    # now = int(time.time())-86400*inter
    # timeArray = time.localtime(now)
    # otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
    # print otherStyleTime
    # program = '军情解码'
    # sqlcursor.execute('''SELECT program_id from competition_analysis where program = '%s';''', program)
    # bufferTemp = sqlcursor.fetchone()
    # program_id = bufferTemp[0]
    # # row_prefix, limit可以限定次数
    # for key,data in table.scan(limit = 10, batch_size = 10):
    #     # print 'hhh',key,data
    # # for key,data in table.scan(row_prefix = 'row', limit = 10, batch_size = 10):
    #     date_created = data['base_info:cdate']
    #     if date_created == otherStyleTime:
    #         content = data['base_info:text']
    #         print 'q',content
    #         # 暂时没有program_id
    #         # program_id = data['base_info:program_id']
    #
    #         count += 1
    #         printCount+=1
    #     # 处理每一条
    #     #     情感关键词
    #         (emotionsWord,emotionsScore) = emProcess.processSentence(rmIrr.removeEverythingButEmotion(content))
    #     #     倾向性判断flag:1是正面，0是中性，-1是负面
    #
    #     #     情感极性判断，这里我限制了更严格的条件
    #         if emotionsScore>0:
    #             tempData.append('1')
    #         elif emotionsScore==0:
    #             tempData.append('0')
    #         elif emotionsScore<0:
    #             tempData.append('-1')
    # #         情感得分sentimentScore
    #         tempData.append(emotionsScore)
    #         # 评论内容
    #         tempData.append(content)
    #     #     日期时间
    #         tempData.append(otherStyleTime)
    #         # 栏目id
    #         tempData.append(program_id)
    #         # 栏目名称
    #         tempData.append(program)
    #     #     转换为元组
    #         commentsData.append(tuple(tempData))
    #         tempData = []
    #         if count>=10:
    #             sqlcursor.executemany('''insert into customer_evaluation(flag, evaluation, content, date, program_id, program)
    #                         values (%s, %s, %s, %s, %s, %s)''',commentsData)
    #             sqlConn.commit()
    #             commentsData = []
    #             count = 0
    #             print '插入'+str(printCount)+'个'
    # # # except:
    # # #      print tempData
    # sqlcursor.executemany('''insert into customer_evaluation(flag, evaluation, content, date, program_id, program)
    #                     values (%s, %s, %s, %s, %s, %s)''',commentsData)
    sqlConn.commit()
    sqlConn.close()


if __name__=='__main__':
    commentTest = addcustomerEvaluation_informal(hbaseIP = '192.168.168.41', mysqlhostIP = '10.3.3.182', dbname = 'btv')

    
