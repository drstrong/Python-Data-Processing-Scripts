#MySQL Import Data
#By Eric Strong
#Last modified: 2016/05/18
#
#This program is meant to import CSV data into a MySQL database

import gzip
import pandas
from datetime import datetime
from os import path, listdir
from sqlalchemy import create_engine, MetaData, TEXT, Integer, Float, Table, Column

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong\Desktop\Anonymized Data"

#START PROGRAM
fileList = listdir(outputDirectory)
startProgramTime = datetime.now();
print("Program initialized at %s" % startProgramTime)
#Create the tables for each file that will be imported
engine = create_engine("mysql+pymysql://root:"+'1127Benfield'+"@localhost/test_db")
meta = MetaData(bind=engine)
for filename in fileList:
    new_table = Table(filename.split(".")[0].lower(), meta,
    Column('DateTime', Integer, primary_key=True, autoincrement=False),
    Column('Value', Float, nullable=True),
    Column('Status', TEXT, nullable=True)) 
meta.create_all(engine)

#Add each data file to the database
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
        #Remove the duplicates in the primary key "DateTime"
        df = df.groupby(df["DateTime"]).first()
        #Push to the db
        df.to_sql(filename.split(".")[0].lower(),engine,flavor='mysql', if_exists='append',index=True)
        #Print progress
        print("%s complete in %s minutes" % (filename,round((datetime.now()-startTagTime).total_seconds()/60,3))) 