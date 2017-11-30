#coding:UTF-8
'''
Created on 2016年3月25日

@author: Ivy

准备数据

'''
import happybase
# 连接hbase数据库
conn = happybase.Connection('192.168.168.41')

# table=conn.table('WEIBO_POST_JQJM')
conn.open()
# 全球零距离
table=conn.table('WEIBO_POST_GQLJL')
# table.put('001', {'base_info:text': '军情解码是一个专业性良好的栏目'})
# 以下是WEIBO_POST_JQJM
# table.put('002', {'base_info:acount': '78', 'base_info:ccount' : '57', 'base_info:cdate' : '2016-02-29', 'base_info:detail_url'\
# : 'urll2', 'base_info:flash':'98', 'base_info:geo': 'hangzhou', 'base_info:has_link' : 'no', 'base_info:hot' :'no', 'base_info:match'\
# :'match', 'base_info:platform': 'ipad', 'base_info:rcount' : '43','base_info:retweet':'ccc', 'base_info:sentiment':'keywords and sentiment',\
# 'base_info:source_name': 'fenghuang', 'base_info:source_url':'uuu','base_info:text':'很讨厌', 'base_info:user_id': '203'})
#
# table.put('003', {'base_info:acount': '58', 'base_info:ccount' : '47', 'base_info:cdate' : '2016-02-29', 'base_info:detail_url'\
# : 'urll2', 'base_info:flash':'47', 'base_info:geo': 'qingdao', 'base_info:has_link' : 'no', 'base_info:hot' :'no', 'base_info:match'\
# :'match', 'base_info:platform': 'ipad', 'base_info:rcount' : '23','base_info:retweet':'ccc', 'base_info:sentiment':'keywords and sentiment',\
# 'base_info:source_name': 'tengxun', 'base_info:source_url':'uuu','base_info:text':'是一档军事节目', 'base_info:user_id': '204'})


# table.put('002-souhu-20160325', {'base_info:abstract': '下任菲律宾总统很可能会调整南海政策与对华关系。除了菲律宾，美国在越南可能也会遭遇挫败。越共十二大决定越共总书记阮富仲续任，而被认为在南海问题上立场比较强硬、对美关系方面持积极立场的阮晋勇则将于年内总理任期结束后退休。', \
# 'base_info:author' : '沙洋波 UM016', 'base_info:datetime' : '2016-03-25 07:34:48', 'base_info:from'\
# : '环球网', 'base_info:title':'越菲将换亲华领导人 美阴谋落空南海或将平静', 'base_info:type': 'none', 'base_info:website' : 'http://mil.sohu.com/20160325/n442006283.shtml'})
#
#
# table.put('002-souhu-20160325',{'body:content' :'对奥巴马政府来说，最后一年任期最主要外交任务之一，可能就是巩固其标志性的外交遗产“亚太再平衡”的战略成果。而今年美国、菲律宾和越南三国的领导人交替，让美国“亚太再平衡”恐生变数。'})

# 全球零距离 关键字微博
table.put('001', {'base_info:acount': '78', 'base_info:ccount' : '57', 'base_info:cdate' : '2016-02-29', 'base_info:detail_url'\
: 'urll2', 'base_info:flash':'98', 'base_info:geo': 'hangzhou', 'base_info:has_link' : 'no', 'base_info:hot' :'no', 'base_info:match'\
:'match', 'base_info:platform': 'ipad', 'base_info:rcount' : '43','base_info:retweet':'ccc', 'base_info:sentiment':'keywords and sentiment',\
'base_info:source_name': 'fenghuang', 'base_info:source_url':'uuu','base_info:text':'很讨厌', 'base_info:user_id': '203'})

table.put('002', {'base_info:acount': '45', 'base_info:ccount' : '67', 'base_info:cdate' : '2016-02-29', 'base_info:detail_url'\
: 'urll2', 'base_info:flash':'106', 'base_info:geo': 'qingdao', 'base_info:has_link' : 'no', 'base_info:hot' :'no', 'base_info:match'\
:'match', 'base_info:platform': 'ipad', 'base_info:rcount' : '78','base_info:retweet':'ccc', 'base_info:sentiment':'keywords and sentiment',\
'base_info:source_name': 'fenghuang', 'base_info:source_url':'uuu','base_info:text':'很好', 'base_info:user_id': '222'})

table.put('003', {'base_info:acount': '58', 'base_info:ccount' : '47', 'base_info:cdate' : '2016-02-29', 'base_info:detail_url'\
: 'urll2', 'base_info:flash':'47', 'base_info:geo': 'qingdao', 'base_info:has_link' : 'no', 'base_info:hot' :'no', 'base_info:match'\
:'match', 'base_info:platform': 'ipad', 'base_info:rcount' : '23','base_info:retweet':'ccc', 'base_info:sentiment':'keywords and sentiment',\
'base_info:source_name': 'tengxun', 'base_info:source_url':'uuu','base_info:text':'是一档军事节目', 'base_info:user_id': '204'})