# -*- coding: utf-8 -*-
import numpy as np
import scipy as sp
import sklearn
from sklearn import tree
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import classification_report
from sklearn.cross_validation import train_test_split
from IPython.display import Image
import pydotplus
import pydot
from sklearn.externals.six import StringIO
''' 数据读入 '''
data = []
labels = []
# 0.15.2
print sklearn.__version__
# from sklearn import datasets
# iris = datasets.load_iris()
# print iris.feature_names
# print iris.target_names

# with open("E:\ctvit\lab\Project\data.txt") as ifile:
with open("E:\ctvit\lab\Project\data.csv") as ifile:
    for line in ifile:
        # print line
        # tokens = line.strip().split(' ')
        tokens = line.strip().split(',')
        data.append([float(tk) for tk in tokens[:-1]])
        labels.append(tokens[-1])
# print "yyyy", data
x = np.array(data)
# print 'eee',type_of_target(x)
labels = np.array(labels)
# print 'rrr',labels, type(labels)
y = np.zeros(labels.shape)
# print "hhhh", y

# ''' 标签转换为0/1 '''
# y[labels == 'fat'] = 1
y[labels == 'yes'] = 1
# y[labels == 'no'] = 0

''' 拆分训练数据与测试数据 '''
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)

''' 使用信息熵作为划分标准，对决策树进行训练 '''
clf = tree.DecisionTreeClassifier(criterion='entropy')
# print 'clf', (clf)
clf.fit(x_train, y_train)
feature_names = ['频数','评论重复率']
''' 把决策树结构写入文件 '''
with open("E:\ctvit\lab\Project\\tree.dot", 'w') as f:
    f = tree.export_graphviz(clf, out_file=f,feature_names=feature_names,max_depth = 2)


# target_names = ['yes','no']
print tree.export_graphviz.__doc__
# print write_png.__doc__
# dot_data = tree.export_graphviz(clf, out_file=None,feature_names=feature_names,max_depth = 5)
# dot_data = tree.export_graphviz(clf,feature_names=feature_names)
# dotfile = StringIO()
# tree.export_graphviz(clf, out_file = dotfile)
# graph = pydotplus.graph_from_dot_data(dotfile.getvalue())
# print graph
# Image(graph.create_png("E:\ctvit\lab\Project\dtree.png"))
# print Image(graph.write_png())
''' 系数反映每个特征的影响力。越大表示该特征在分类中起到的作用越大 '''
print 'feature_importances_',(clf.feature_importances_)

'''测试结果的打印'''
answer = clf.predict(x_train)
# print(x_train)
# print"answer", (answer)
# print(y_train)
print '拟合准确率', (np.mean(answer == y_train))

'''准确率与召回率'''
# precision, recall, thresholds = precision_recall_curve(y_train, clf.predict(x_train))
# print "clf.predict_proba(x)", clf.predict_proba(x)
answer = clf.predict(x_test)
# print '5.准确率与召回率', (classification_report(y, answer, target_names=['fat', 'thin']))
print '5.准确率与召回率', (classification_report(answer,y_test))