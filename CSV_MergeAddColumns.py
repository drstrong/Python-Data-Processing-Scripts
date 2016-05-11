#CSV Merge By Adding Columns
#By Eric Strong
#Last modified: 2016/05/05
#
#This program will join CSV files with different variable names, adding
#additional columns to a CSV file.

import pandas
from os import path
from os import listdir
from datetime import datetime

#INPUT PARAMETERS
workingDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"
newFilename = "_MergedData_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"

def MergeByColumn(fileList):
    df = pandas.DataFrame();
    for fileName in fileList:
        pathName = path.join(workingDirectory,fileName)
        df = pandas.merge(left = df, 
                     right = pandas.read_csv(pathName, 
                                             header=None, 
                                             index_col = 0, 
                                             names = [fileName.split('_')[0]],
                                             parse_dates = True), 
                     left_index = True, 
                     right_index = True, 
                     how = "outer")
    df.fillna(method='pad',inplace=True)
    #Write results to file
    outputPath = path.join(workingDirectory,newFilename)
    df.to_csv(outputPath, header = True)

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
#Build a list of files that meet the correct criteria
fileList = [f for f in listdir(workingDirectory) if ((
                                                    f.endswith(".csv") or 
                                                    f.endswith(".gz"))
                                                    and not f.startswith("_"))]
MergeByColumn(fileList)
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 