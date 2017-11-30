#coding=UTF-8
import jieba

# line = '中国版AK-47步枪海外热销 在美国被卖出别墅价'
line = '美军F-35C首次成功降落航母 未采用垂直起 中新网11月4日电'
line = jieba.cut(line,cut_all = False)
for i in line:
    print i