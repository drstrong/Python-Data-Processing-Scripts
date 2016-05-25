#Files- CSV Lines to Batch
#By Eric Strong
#Last modified: 2016/05/06
#
#This program is meant to output a list of files in a directory.

import pandas
import numpy
from os import path
from os import listdir
from datetime import datetime

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"
inputFilename = "SplittingCSV.csv"
outputFilenamePrefix = "MDC_Split_"
numGroups = 400

#START PROGRAM
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
fileList = listdir(outputDirectory)
fileLines = pandas.read_csv(path.join(outputDirectory,inputFilename))
fileLinesSplit = numpy.array_split(fileLines.values,numGroups)
ii = 0
for fileStrings in fileLinesSplit:
    ii = ii + 1
    with open(path.join(outputDirectory, outputFilenamePrefix + str(ii) + ".bat"),'w') as outputFile: 
        for fileLine in fileStrings:
            outputFile.write(fileLine[0] + "\n")   
print("Program finished in %s minutes" % round((datetime.now()-startProgramTime).total_seconds()/60,3)) 