#CSV Data Info
#By Eric Strong
#Last modified: 2016/05/24
#
#This program is meant to summarize metrics for a CSV data file. Main Functionality- 
#find filename, filesize, start and ending dates, summarizing metrics, and data gaps

import csv
import gzip
import pandas
from os import path, listdir
from datetime import datetime

#INPUT PARAMETERS
#I/O
outputDirectory = r"C:\Users\estrong\Desktop\CWD"
outputFilename = "_DataSummary_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
outputGapPrefix = "_gaps_"
dateUnit = 'ms'
#Summarizing Data
summarizeData  = True
#Data Gaps
findDataGaps = False
dataGapDef = 1200 #in seconds

def FindDataGaps(df,filename): 
    outputGapFilename = outputGapPrefix + filename
    with open(path.join(outputDirectory, outputGapFilename),'w',newline='') as outGapFile:    
        gapWriter = csv.writer(outGapFile)
        prevDate = df.head(1)['DateTime'].tolist()[0]   
        for curDate in df['DateTime'].tolist():
            elapsedSeconds = (curDate - prevDate).total_seconds()                                 
            if (elapsedSeconds > dataGapDef):
                gapWriter.writerow((prevDate, curDate))        
            prevDate = curDate;

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
fileList = listdir(outputDirectory)
#Open the results file
with open(path.join(outputDirectory, outputFilename),'w',newline='') as outputFile:
    resultsWriter = csv.writer( outputFile )
    #Write the header
    if (not summarizeData): resultsWriter.writerow(('Filename','Start Date','End Date','Size(MB)'))  
    else: resultsWriter.writerow(('Filename','Start Date','End Date','Size(MB)','Count','Mean','Std','Min','25%','50%','75%','Max')) 
    #Iterate over each file in directory    
    for filename in fileList:  
        startTagTime = datetime.now(); 
        inputPath = path.join(outputDirectory, filename)
        fileSize = round(path.getsize(inputPath)/1000000,4)        
        #Basic error checking
        if (path.getsize(inputPath) > 0 and not inputPath.startswith("_")):
            #Only raw CSV and gzipped CSV files are supported    
            if inputPath.endswith(".csv"): csvFile = open(inputPath,'r')            
            elif inputPath.endswith(".gz"): csvFile = gzip.open(inputPath,'rb')  
            else: continue
            #Open the file as a pandas dataframe   
            df = pandas.read_csv(csvFile, index_col=0,header=None, names = ["DateTime","Value","Status"])      
            #Extract the starting and ending times
            startTime = datetime.fromtimestamp(df.head(1).index[0]).strftime("%Y-%m-%dT%H%M%S")
            endTime = datetime.fromtimestamp(df.tail(1).index[0]).strftime("%Y-%m-%dT%H%M%S")       
            #Data gaps
            if (findDataGaps): FindDataGaps(df,filename)
            #Write the results
            if (not summarizeData): resultsWriter.writerow((filename,startTime, endTime,fileSize))
            else: resultsWriter.writerow(list((str(filename),startTime, endTime,fileSize)) + df.describe()['Value'].tolist())
            print("%s complete in %s minutes" % (filename,round((datetime.now()-startTagTime).total_seconds()/60,3))) 
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 