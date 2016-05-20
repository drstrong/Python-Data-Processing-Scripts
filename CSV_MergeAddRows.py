#CSV Merge By Adding Rows
#By Eric Strong
#Last modified: 2016/05/05
#
#This program will join CSV files with the same variable name and sort them.

import pandas
from os import listdir, path
from datetime import datetime

#INPUT PARAMETERS
workingDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"

#Build a list of keys (CSV files that share the same eDNA tag, to be joined)
def BuildKeys(fileList):
    csv_files = {}
    for fileName in fileList:
        #Error checking
        if ((fileName.endswith(".csv") or fileName.endswith(".gz")) and not fileName.startswith("_")):
            key = path.splitext(path.basename(fileName))[0].split('_')[0]
            csv_files.setdefault(key, []).append(fileName)
    return csv_files

#Merge a list of files by concatenating them along their rows
def MergeByRow(csv_files):
    #Iterate over all the files in the keylist
    for key,filelist in csv_files.items(): 
        #If the length of the filelist is not greater than 1, no files to join
        if (len(filelist) > 1):    
            startTagTime = datetime.now()
            #Build a list of all file paths using the filelist and working directory
            pathList = [path.join(workingDirectory,f) for f in filelist]
            #Concatenate all the files
            df = pandas.concat(pandas.read_csv(f, header=None, parse_dates = False, index_col = 0, names = ["DateTime", key, "Status"]) for f in pathList)
            df.sort_index(axis = 0, ascending = True, inplace = True)
            #Intermediate processing
            startTime = datetime.fromtimestamp(df.head(1).index[0]).strftime("%Y-%m-%dT%H%M%S")
            endTime = datetime.fromtimestamp(df.tail(1).index[0]).strftime("%Y-%m-%dT%H%M%S")        
            newFilename = '%s_%s_to_%s.csv' % (key, startTime, endTime)
            newFilePath = path.join(workingDirectory,newFilename)
            #Write to a new file  
            df.to_csv(newFilePath, index = True, header = False)
            print("%s complete in %s minutes" % (newFilename,round((datetime.now()-startTagTime).total_seconds()/60,3))) 

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
MergeByRow(BuildKeys(listdir(workingDirectory)))
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 