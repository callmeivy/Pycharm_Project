#coding=UTF-8
import os
import jieba
# jieba.load_userdict('D:\QMDownload\data-for-1.3.3\data\dictionary\custom\机构名词典')

# 先将所有的txt合并为一个
#获取目标文件夹的路径
# path = "D:\QMDownload\data-for-1.3.3\data\dictionary\custom"
import jieba.posseg as pseg
def combine_files():
    path = "D:\\test"
    filenames = os.listdir(path)
    print filenames
    f=open('customize_dict.txt','w')
    for file in filenames:
        file = path + "\\" + file
        for line in open(file):
            f.writelines(line)
        f.write('\n')
    f.close()





path = "customize_dict.txt"
jieba.load_userdict(path)


test_sent = ("马良大院在我国东北部")
# test_sent = ("小龙矿区管委会在我国东北部")
words = jieba.cut(test_sent)
print('/'.join(words))
full_text = pseg.cut(test_sent)
for i in full_text:
    print i.word,i.flag
    if i.flag == 'x':
        print 'lll',i.word,i.flag

# if __name__=='__main__':
#     # combine_files()