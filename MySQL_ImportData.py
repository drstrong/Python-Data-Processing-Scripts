#MySQL Import Data
#By Eric Strong
#Last modified: 2016/05/18
#
#This program is meant to import CSV data into a MySQL database

from sqlalchemy import create_engine, MetaData, TEXT, Integer, Table, Column, ForeignKey 
from os import listdir

#INPUT PARAMETERS
outputDirectory = r"C:\Users\estrong.THE_DEI_GROUP\Desktop\CWD"

#START PROGRAM
fileList = listdir(outputDirectory)
#Create the tables for each file that will be imported
engine = create_engine("mysql+mysqldb://root:"+'1127Benfield'+"@localhost/test_db")
meta = MetaData(bind=engine)
for filename in fileList:
    new_table = Table('actors', meta,
    Column('actor_id', Integer, primary_key=True, autoincrement=False),
    Column('Actor Name', TEXT, nullable=True),
    Column('Featured in dead parrot sketch', TEXT, nullable=True),
    Column('num characters played in sketch', TEXT, nullable=True)) 
meta.create_all(engine)

#Add each data file to the database
for filename in fileList:  
    parrot_actor_df.to_sql('actors',engine,flavor='mysql', if_exists='append',index=True)
    