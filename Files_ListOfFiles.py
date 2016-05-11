#Files- List of Files
#By Eric Strong
#Last modified: 2016/05/06
#
#This program is meant to output a list of files in a directory.

import csv
from os import path
from os import listdir
from datetime import datetime

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"
outputFilename = "_ListOfFiles_" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
fileList = listdir(outputDirectory)
with open(path.join(outputDirectory, outputFilename),'w') as outputFile:
    resultsWriter = csv.writer( outputFile )    
    for f in fileList:
        resultsWriter.writerow(f)
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 