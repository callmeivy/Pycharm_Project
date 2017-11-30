#coding UTF-8
# UniqueVisitor
import csv
import time

st = time.time()

asset = []
# UV:65936
user_program = dict()
ele_box = list()
count2 = 0

count = 0
tun = list()
asset_pro = dict ()
def fun(asset_ele,ele_box_y):
    for oneAsset in asset_ele:
        prog_list = list()
        for one_elemnet in ele_box_y:
            programID = one_elemnet.split(',')[0]
            userID = one_elemnet.split(',')[1]
            if programID == oneAsset:
                if userID not in prog_list:
                    prog_list.append(userID)


        length = len(prog_list)

        asset_pro[oneAsset] = prog_list
    return asset_pro

# f = open('E:\EdgeA_Viewing History(D).01_01-ZQ.20150317.20150318080006ori.csv','rU')
with open('E:\EdgeA_Viewing History(D).01_01-ZQ.20150317.20150318080006ori.csv','rU') as f:
    for line in f:

        cells = line.split( "," )
        if cells[21] not in asset:
            asset.append((cells[21]))

        # 9 is caid,21 is program id

        element = cells[21]+','+cells[9]
        ele_box.append(element)
        # count2 += 1
        # if count2 > 10:
        #     break

print fun(asset,ele_box)







print "Finish!!!!!!!!!!!!!!"
# f.close()

et = time.time()
print int(et)- int(st)