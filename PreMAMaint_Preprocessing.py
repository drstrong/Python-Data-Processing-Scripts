#PreMAMaint Preprocessing
#By Eric Strong
#Last modified: 2016/05/05
#
#This program will modify the dateTime from epoch time to the standard form, delete
#the first row of the csv file (which is redundant because the signal name is the filename),
#and rename the filename (personal preference)

import csv
import gzip
import shutil
import pandas
import os
from os import path
from os import listdir
from datetime import datetime

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"
outputFilename = "_PreprocessingResults_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
delFirstColumn = True
dateTimeMod = True
dateUnit = 'ms'
rename = True
zipResults = True
removeNonZippedFile = False

#A function to construct a new path from the INPUT PARAMETERS
def construct_new_path(outputDirectory, filename, rename):   
    outputFilename = path.splitext(filename)[0] + "_out.csv"
    if (rename):
        splitFile = path.splitext(filename)[0].split('_')
        if len(splitFile)>4:
            outputFilename = "_".join(("".join(splitFile[4:]),splitFile[0],splitFile[1],splitFile[2])) + ".csv"   
    return (path.join(outputDirectory,outputFilename),outputFilename)

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
fileList = listdir(outputDirectory)
#Open the results file
with open(path.join(outputDirectory, outputFilename),'w',newline='') as outputFile:
    resultsWriter = csv.writer( outputFile )
    #Write the header
    resultsWriter.writerow(('Filename','Start Date','End Date','Size(MB)'))  
    #Iterate over each file in directory    
    for filename in fileList:  
        startTagTime = datetime.now(); 
        inputPath = path.join(outputDirectory, filename)
        outputPath, outputFilename = construct_new_path(outputDirectory, filename, rename)  
        fileSize = path.getsize(inputPath)
        #Error checking
        if (fileSize > 0 and inputPath.endswith(".csv") and not inputPath.startswith("_")):
            with open(inputPath,'r') as inputFile: 
                fileSize = round(fileSize/1000000,4)
                #Open the file as a pandas dataframe        
                df = pandas.read_csv(inputFile, header=None, names=['Tag','DateTime','Value']) 
                #Extract the starting and ending times
                startTime = pandas.to_datetime(df.head(1)['DateTime'],unit=dateUnit).tolist()[0]
                endTime = pandas.to_datetime(df.tail(1)['DateTime'],unit=dateUnit).tolist()[0]       
                #Additional options which may be selected
                if (delFirstColumn): df = df.loc[:,['DateTime','Value']]
                if (dateTimeMod): df['DateTime'] = pandas.to_datetime(df['DateTime'],unit=dateUnit)      
                #Write the results
                df.to_csv(outputPath, index = False, header = False)
                resultsWriter.writerow((outputFilename,str(startTime), str(endTime), fileSize))  
                #If zipping the results
                if (zipResults):
                    with open(outputPath, 'rb') as f_in, gzip.open(outputPath + ".gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out) 
                    if (removeNonZippedFile): os.remove(outputPath)  
            print("%s complete in %s minutes" % (filename,round((datetime.now()-startTagTime).total_seconds()/60,3))) 
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 