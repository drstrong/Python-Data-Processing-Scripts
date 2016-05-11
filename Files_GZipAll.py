#Files_GZipAll
#By Eric Strong
#Last modified: 2016/05/03
#
#This program will gzip all the files inside a directory.

import os
import datetime
import gzip
import shutil

#INPUT PARAMETERS
cWD = 'C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD'

#START PROGRAM
startProgramTime = datetime.datetime.now();
print(str("Program initialized at " + str(startProgramTime)))
os.chdir(cWD)
fileList = os.listdir(cWD)
#Iterate over each file in directory   
for filename in fileList:
    #Initialization
    startFileTime = datetime.datetime.now();
    outputPath = os.path.join(cWD,filename)    
    #GZip the current file
    with open(outputPath, 'rb') as f_in, gzip.open(outputPath + ".gz", 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)  
    #Remove the original file after zipping    
    os.remove(outputPath)    
    #Print the elapsed time
    minutesElapsed = round((datetime.datetime.now() - startFileTime).total_seconds()/60,3)
    print(str(filename + " in " + str(minutesElapsed) + " minutes"))    
#Calculate the total time the program took to run
totMinutesElapsed = (datetime.datetime.now() - startProgramTime).total_seconds()/60;
print(str("Program finished in " + str(totMinutesElapsed) + " minutes"))