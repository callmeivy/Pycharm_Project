# coding=UTF-8
import MySQLdb
#mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="demo_vsp_a")
mysqlconn=MySQLdb.connect(host="172.16.168.57",user="ire",passwd="ZAQ!XSW@CDE#",db="ire")
cur=mysqlconn.cursor()
#cur.execute("""select id from catalog_info where parent_id=0""")
#id=cur.fetchall()
i=0
s=0
t=0
#for row in id:
    #i+=1
    #print row,i

###type
#def typesearch():
    #cur.execute("""select type from catalog_info where id=%s""",(row))
    #type=cur.fetchall()
    #if type==0:
        #s+=1
    #elif type==1:
        #t+=1
    #print "type",type
###sort_index
#def sort_indexsearch():
#################################
#将所有的子节点插入到列表中，每次读取列表的第0个元素查找对应的type
#不全node=[10000002,10000003,10000004,10000005,10000006,10000007,10000008,10000009,10000010,10000011,10000012,10000013,10000111,10000112,10000113,10000114,10000115,10000116,10000117,10000118,10000119,10000120,10000121,10000122,10000123,10000124,10000125,10000126,10000127,10000128,10000129,10000130,10000131,10000132,10000133,10000134,10000135,10000136,10000137,10000138,10000139,10000140,10000141,10000142,10000143,10000144,10000145,10000146,10000147,10000148,10000149,10000150,10000151,10000152,10000153,10000154,10000155,10000156,10000157,10000158,10000159,10000160,10000161,10000162,10000163,10000164,10000165,10000166,10000167,10000168,10000169,10000170,10000171,10000172,10000173,10000174,10000175,10000176,10000177,10000178,10000179,10000180,10000181,10000182,10000183,10000184,10000185,10000186,10000187,10000188,10000189,10000190,10000191,10000192,10000193,10000194,10000195,10000196,10000197,10000198,10000199,10000200,10000201,10000202,10000203,10000204,10000205,10000206,10000207,10000208,10000209,10000210,10000211,10000212,10000213,10000214,10000215,10000216,10000217,10000218,10000219,10000220,10000221,10000222,10000223,10000224,10000225,10000226,10000227,10000228,10000229,10000230,10000231,10000232,10000233,10000234,10000235,10000236,10000237,10000238,10000239,10000240,10000241,10000242,10000243,10000244,10000245,10000246,10000247,10000248,10000249,10000250,10000251,10000252,10000253,10000254,10000255,10000256,10000257,10000258,10000259,10000260];
node=[1000108135,1000034400,1000023201,1000022490,1000016757,1000016731,1000009016,1000009015,10014817,10000094,10000095,10000096,10000097,10000098,10000099,10000169,10000228,10000245,10000246,10000247,10000248,10000227,10000257,10000258,10000259,10000260,10000261,10000262,10000263,1000108299,1000108300,1000108301,1000108302,1000108303,1000108304,1000045625,1000107144,1000107145,1000108305,1000107752,1000108306,1000103064,1000045630,1000022783,1000039146,1000027649,1000034083,1000022784,1000039148,1000022274,1000038068,1000038069,1000038070,1000039149,1000038071,1000038072,1000038073,10000381,10000382,10000384,10000383,10000385,10000386,10000387,10000411,10000412,10000413,10000414,10000415,10000416,10000417,10001475,10001062,10001314,10001333,10001352,10001371,10001390,10001233,10001409,10001436,10001463,10001464,10001467,10001990,10001991,10001992,10001993,10001994,10001995,10001996,10001997,10001998,10001999,10002000,10002001,10002002,10002003,10002004,10001649,10001650,10001651,10001654,10001652,10001653,10001655,10001656,10001657,10001658,10001659,10001811,10001812,10005220,10005221,10017284,10001852,10001846,10001720,10001721,10001724,10001723,10001722,10001725,10001726,10008493,10017509,10001770,10001771,10015192,10015193,10015194,10015195,10015196,10015197,10015198,10015199,10015200,10015201,10015202,10015203,10015204,10015205,10015206,10015207,10015208,10015209,10015210,10015211,10015212,10015213,10015214,10015697,10015698,10015699,10017279,10017280,10017281,10017282,10017283,10017567,10017568,10017569,10017570,10017571,10017572,10017573,10017574,10017575,10017576,10017577,10017578]
print "node[0]",node[0]
j=0
m=0
n=0
for j in range (0,50000):
    if len(node)>j:
        print "j:",j,"node:",node[j],"type:",type
        print len(node)
        #assert len(node)>j
        cur.execute("""select sort_index from catalog_info where id=%s""",(node[j]))
        sort_index=str(cur.fetchall())
        sort_index1=sort_index.split(';')
        for sonrow in sort_index1:
            #print sonrow
            if sonrow not in node:
                node.append(sonrow)
        cur.execute("""select type from catalog_info where id=%s""",(node[j]))
        #type=cur.fetchall()
        type=cur.fetchone()
        #if type==0:
        #if type=='0':
        #if int(type)==0:
        #print "j:",j,"node:",node[j],"type:",type
        j+=1

        if type is not None:
            #if cmp(type[0],'1')==0:
            if cmp(type[0],'1')==0:
            #if type=='1':
                m+=1
            #elif type=="1":
            #elif type=='1':
            else:

                n+=1

print "m",m,"n",n
cur.close()
mysqlconn.close()