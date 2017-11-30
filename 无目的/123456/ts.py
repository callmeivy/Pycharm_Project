

with open('C:\\appStep1.txt','r') as fp:
    lines = fp.readlines()
    for line in lines:
        line = line.strip().split('\t')
        tempLine = eval(line[1])
        # rec users
        recDict = dict()
        for ids in tempLine:
            ids = ids.strip()
            print ids