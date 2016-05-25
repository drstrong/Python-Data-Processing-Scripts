#Anonymize Data
#By Eric Strong
#Last modified: 2016/05/18
#
#This program is meant to modify a list of CSV files within a directory so that
#the data is anonymized.

import random
import gzip
import pandas
from os import path, listdir
from datetime import datetime

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"
timeOffset = 66200

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
fileList = listdir(outputDirectory)
for filename in fileList:  
    startTagTime = datetime.now(); 
    inputPath = path.join(outputDirectory, filename)
    outputPath = inputPath + "_anon.csv";
    fileSize = round(path.getsize(inputPath)/1000000,4)        
    #Basic error checking
    if (path.getsize(inputPath) > 0 and not inputPath.startswith("_")):
        #Only raw CSV and gzipped CSV files are supported    
        if inputPath.endswith(".csv"): csvFile = open(inputPath,'r')            
        elif inputPath.endswith(".gz"): csvFile = gzip.open(inputPath,'rb')  
        else: continue
        #Open the file as a pandas dataframe   
        df = pandas.read_csv(csvFile, header=None, names = ["DateTime","Value","Status"])  
        df["DateTime"] = df["DateTime"] + 66200
        multiplyNum = random.random() + 0.5
        offset = random.random()*10 - 10
        df["Value"] = df["Value"] * multiplyNum + offset
        df.to_csv(outputPath,header=False)
        print(filename)