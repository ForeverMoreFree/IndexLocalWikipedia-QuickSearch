import csv, pickle,os,sys
import os.path
import pandas as pd
import numpy as np
import re
from multiprocessing.dummy import Pool as ThreadPool
import time

def pickledump(filename,data):
    pickling_on = open('WikiArticleIndicies/'+filename+".p","wb")
    pickle.dump(data, pickling_on)
    pickling_on.close()

def timeToSearchDicts():   #Testing how long it takes to create one million entries
    timestart=time.time()
    TestDict={}
    for x in range(1000000):
        TestDict[x]=[285, 286, 287, 292, 293, 294, 295, 296, 297]
    timeElapsed=time.time()-timestart
    print(timeElapsed)
    timestart=time.time()
    TestList=[]
    for x in range(0,1000000,1000):
        TestList.append(TestDict[x])
    timeElapsed=time.time()-timestart
    print(timeElapsed)

def loadWikiArticles():
    dataFrame = pd.read_csv('WikiDataStorage/articles.csv')
    articleList = pd.Series.tolist(dataFrame)
    newList = []
    alteredList = []
    for rowNum, rowData in enumerate(articleList):
        for x in range(1, len(rowData)):
            if not type(rowData[x]) == type(''): rowData[x] = ''
            tempVar = rowData[x].lower()
            tempVar = tempVar.replace("(", " ")
            tempVar = tempVar.replace(")", " ")
            tempVar = tempVar.replace("?", " ")
            tempVar = tempVar.split("/")
            tempVar = " ".join(tempVar)
            alteredList.append(tempVar.split(" "))
            newList.extend(tempVar.split(" "))
    newList = sorted((list(set(newList))))
    newList = newList[1:]
    return newList,alteredList

def loadFrequentWords(newList,alteredList):
    df = pd.read_csv('WikiDataStorage/top75000words.csv',"ISO-8859-1")
    temp=pd.Series.tolist(df)
    LoadedTopWords=[]
    for rowNum,rowData in enumerate(temp):
        LoadedTopWords.append(str(rowData[0]))
    LoadedTopWords.extend(newList)
    listToThread=[]
    for x, item in enumerate(LoadedTopWords):
        listToThread.append([item,alteredList,x])
    return listToThread

def searchWord(myArgs):
    word = myArgs[0]
    timestart = time.time()
    if not os.path.isfile('WikiArticleIndicies/'+word+".p"):
        alteredList=myArgs[1]
        rowSearchNum=myArgs[2]
        indexDict={}
        # for x, word in enumerate(newList):
        indexDict[word] = []
        for rowNum, rowData in enumerate(alteredList):
            for y in range(len(rowData)):
                indicies = [m.start() for m in re.finditer(word, rowData[y])]
                if len(indicies) > 0:
                    indexDict[word].append(rowNum)
                    break
        timeElapsed = time.time() - timestart
        print('Time: {0:.3g}'.format(timeElapsed),"Row:", rowSearchNum,"  ", word, " x ",len(indexDict[word]))
        pickledump(word,{word:indexDict[word]})

newList, alteredList=loadWikiArticles()
listToThread = loadFrequentWords(newList,alteredList)

pool = ThreadPool(1)
pool.map(searchWord, listToThread)
pool.close()
pool.join()








