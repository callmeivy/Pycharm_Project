# coding=UTF-8
import MySQLdb
#mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="demo_vsp_a")
#mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
mysqlconn=MySQLdb.connect(host="10.3.3.220",user="root",passwd="123456",db="gehua")
cur=mysqlconn.cursor()
#cur.execute("""select id from catalog_info where parent_id=0""")
#id=cur.fetchall()
#print id
#for rows in id:
    #cur.execute("""select sort_index from catalog_info where id=%s""",(rows))
    #sorted_id=cur.fetchall()
    ######将type为0的取出
node=[1000108135,1000034400,1000023201,1000022490,1000016757,1000016731,1000009016,1000009015,10014817,10000094,10000095,10000096,10000097,10000098,10000099,10000169,10000228,10000245,10000246,10000247,10000248,10000227,10000257,10000258,10000259,10000260,10000261,10000262,10000263,1000108299,1000108300,1000108301,1000108302,1000108303,1000108304,1000045625,1000107144,1000107145,1000108305,1000107752,1000108306,1000103064,1000045630,1000022783,1000039146,1000027649,1000034083,1000022784,1000039148,1000022274,1000038068,1000038069,1000038070,1000039149,1000038071,1000038072,1000038073,10000381,10000382,10000384,10000383,10000385,10000386,10000387,10000411,10000412,10000413,10000414,10000415,10000416,10000417,10001475,10001062,10001314,10001333,10001352,10001371,10001390,10001233,10001409,10001436,10001463,10001464,10001467,10001990,10001991,10001992,10001993,10001994,10001995,10001996,10001997,10001998,10001999,10002000,10002001,10002002,10002003,10002004,10001649,10001650,10001651,10001654,10001652,10001653,10001655,10001656,10001657,10001658,10001659,10001811,10001812,10005220,10005221,10017284,10001852,10001846,10001720,10001721,10001724,10001723,10001722,10001725,10001726,10008493,10017509,10001770,10001771,10015192,10015193,10015194,10015195,10015196,10015197,10015198,10015199,10015200,10015201,10015202,10015203,10015204,10015205,10015206,10015207,10015208,10015209,10015210,10015211,10015212,10015213,10015214,10015697,10015698,10015699,10017279,10017280,10017281,10017282,10017283,10017567,10017568,10017569,10017570,10017571,10017572,10017573,10017574,10017575,10017576,10017577,10017578]
node1=[]
for j in range(0,162):
    cur.execute("""select type from catalog_info where id=%s""",(node[j]))
    type=cur.fetchone()
    #print "type",type[0]
    if type is not None:
        if cmp(type[0],'0')==0:
            node1.append(node[j])
print node1







cur.close()
mysqlconn.close()