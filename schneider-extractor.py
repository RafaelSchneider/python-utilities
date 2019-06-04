#!/usr/bin/env python^M
# -*- coding: utf-8 -*-

import time
import datetime

def mongoConnection():
    from pymongo import MongoClient
    print("Mongo connection...")
    client = MongoClient("here is de db url")
    print("Mongo connected !")
    return client.NameOfDatabase    

def importCsvFiles():
    import pandas as pd
    import glob
    
    path = "/myuser/path/backupOfAllCsvArchives"
    allFiles = glob.glob(path + "/*.csv")
    print("Archives that must be imported => " + str(allFiles))
    
    listOfDataFrames = []

    for filename in allFiles:
        df = pd.read_csv(filename, index_col=None, header=0)
        listOfDataFrames.append(df)

    return pd.concat(listOfDataFrames, axis=0, ignore_index=True)
    
def saveToCsvFile(dataframe):
    start_time = time.time()
    print("Exporting to csv file...Started at "+str(datetime.datetime.now().time()))
    dataframe.to_csv('~/projetos/python/extractor_data/all-data-out.csv')    
    elapsed_time = time.time() - start_time
    print("Done in "+str(datetime.timedelta(seconds=elapsed_time))+" !")

def doFilterInDataFrame(dataframe):

    print("Starting filters on dataframe...Started at "+str(datetime.datetime.now().time()))    
    dataframe.sort_values(by=['date'], inplace=True, ascending=False)
    dataframe = dataframe.drop_duplicates(subset='cpfCnpj')
    print("Filters done. Ready to export !")
    return dataframe

def createCsvFromMongoDb():
    import pandas as pd
    start_time = time.time()

    db = mongoConnection()   
    start = datetime.datetime(2019, 4, 17, 16, 28, 45)
    end = datetime.datetime(2019, 6, 3, 00, 0, 00)
    print("Starting query...")    
    nameOfTable = db.nameOfTable.find({'date':{'$lt': end, '$gte': start}}).limit(10000000)
    print("Query Done !")

    print("Creating an DataFrame...Started at "+str(datetime.datetime.now().time()))
    df = pd.DataFrame(list(nameOfTable), columns=['id', 'cpfCnpj', 'date', 'origin'])
    print("DataFrame created !")
    
    print("Starting filters on dataframe...Started at "+str(datetime.datetime.now().time()))
    df.sort_values(by=['date'], inplace=True, ascending=False)
    df = df.drop_duplicates(subset='cpfCnpj')
    print("Filters done. Ready to export !")   
    
    print("Exporting to csv file...Started at "+str(datetime.datetime.now().time()))
    df.to_csv('~/projetos/python/extractor_data/data-out.csv')    
    elapsed_time = time.time() - start_time
    print("Done in "+str(datetime.timedelta(seconds=elapsed_time))+" !")

def createCsvFromAllFiles():    
    saveToCsvFile(doFilterInDataFrame(importCsvFiles()))

def createCsv(fromMongo):
    print("Creating cvs from...")
    if fromMongo is 1:
        print(" MongoDb.")
        createCsvFromMongoDb()
    else:
        print("all files cvs.")
        createCsvFromAllFiles()

createCsv(fromMongo)
