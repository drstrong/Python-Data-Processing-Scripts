#PreMAMaint Find Data Gaps- Merging Multiple Gaps
#By Eric Strong
#Last modified: 2016/03/16
#
#This program will find the common data gaps between different tags, useful
#for determining eDNA service outages.

import csv
import os
import datetime

#INPUT PARAMETERS
cWD = 'C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD'
resultsFilename = '_DataGapResults_Multiple.csv'
inputFilename = '_DataGapResults.csv'

#START PROGRAM
startProgramTime = datetime.datetime.now();
print str("Program initialized at " + str(startProgramTime))
os.chdir(cWD)
resultsPath = os.path.join(cWD, resultsFilename)
inputPath = os.path.join(cWD, inputFilename)

#Need to load the data in a good format
with open(inputPath,'rb') as inFile:
    firstLine = inFile.readline()
    prevTag = firstLine.split(',')[0]
    csvInReader = csv.reader(inFile);
    
    #Preallocate the lists. "startList" and "endList" are the main overall lists
    #that we want to find. "tagStartList" and "tagEndList" are temporary lists
    #which will be appended to the master lists
    startList = list()
    endList = list()
    tagStartList = list()
    tagEndList = list()     
    
    #Read through each line in the input file. This is easier to parse because
    #we know the tags are subsequent
    for line in csvInReader: 
        
        #If the tag name hasn't changed, we want to add the start and end dates
        #to the temporary tag lists
        if line[0] == prevTag:
            tagStartList.append((line[1],1))
            tagEndList.append((line[2],2))
            
        #Once the tag name changes, we want to append the temporary list to the
        #master list, clear the temporary list, and append the current item to
        #the new temporary list. Also, replace the previous tag with the 
        #current tag
        else:
            startList.append(tagStartList)
            tagStartList = list();
            tagStartList.append((line[1],1))
            endList.append(tagEndList)
            tagEndList = list();
            tagEndList.append((line[2],2))
            prevTag = line[0]

#We're going to create a master list that includes both start and end dates.
#Then iterate, and increment for start times, and decrement for end times.
#Every 0 to 1 indicates a new "start", and every 1 to 0 indicates a new "end".
#See stackoverflow explanation. 
tempDateList = list()
tempDateList.extend(startList[1])
tempDateList.extend(endList[1])

#We need to do this with two lists at a time, the initial list (above), then
#iterated for each list in the input file
for ii in range(2,len(startList)-2):
    
    #We're making a bigger list comprised of two of the tag lists at once
    tempDateList.extend(startList[ii])
    tempDateList.extend(endList[ii])
    tempDateList.sort()
    prevCounter = 0;
    newDateList = list()
    
    #Iterate over all entries, and construct a new, merged list
    for dateIndex in range(1,len(tempDateList)-1):
        
        #If a start date, increment, else, decrement
        if (tempDateList[dateIndex][1] == 1):
            curCounter = prevCounter + 1
        elif (tempDateList[dateIndex][1] == 2):
            curCounter = prevCounter - 1
        
        #Check if a value went from 0 to 1 or 1 to 0
        if (prevCounter == 0) & (curCounter == 1):
            newDateList.append((tempDateList[dateIndex][0],1))
        elif (prevCounter == 1) & (curCounter == 0):
            newDateList.append((tempDateList[dateIndex][0],2))            
        prevCounter = curCounter
    
    #Now that the merging is finished, overwrite the temp list with the new list
    tempDateList = newDateList

#Now go through and remove entries where the start date and end date are the same
#Open the new file
with open(resultsPath,'wb') as outFile:
    resultsWriter = csv.writer( outFile )
    resultsWriter.writerow(('Start Date','End Date'))
    for dateIndex in range(0,len(tempDateList)-2,2):
        if (tempDateList[dateIndex][0] != tempDateList[dateIndex+1][0]):
            resultsWriter.writerow((str(tempDateList[dateIndex][0]),str(tempDateList[dateIndex+1][0])))
        
#Calculate the total time the program took to run            
totMinutesElapsed = round((datetime.datetime.now() - startProgramTime).total_seconds()/60,3)
print str("Program finished in " + str(totMinutesElapsed) + " minutes")

#Delete the unnecessary variables
del(cWD,inputFilename,inputPath,line,prevTag,resultsFilename,resultsPath,ii)
del(firstLine,startProgramTime,tagEndList,tagStartList,totMinutesElapsed)