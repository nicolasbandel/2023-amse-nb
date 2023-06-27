import pandas as pd
import sqlite3
import urllib.request
import os

filePathLocation = "./project/data/locationData.sqlite"
filePathShock = "./project/data/shockData.sqlite"
filePathAutomatedDataPipeline = "./project/AutomatedDataPipeline.py"

def deleteFile(path):
    if os.path.exists(path):
        os.remove(path)
        print("Successfully! The File has been removed")
    else:
        print("Can not delete the file as it doesn't exists")
        
def runAutomatedDataPipeline():
    command = "python " + os.path.abspath(filePathAutomatedDataPipeline)
    os.system(command)
    
def loadDB(path, table):
    query = "Select * from " + table 
    con = sqlite3.connect(path)
    df = pd.read_sql_query(sql=query, con=con)
    con.close()
    return df
    
def testDB(path, table, colums):
    try:
        print("test: " + path)
        #check file
        assert os.path.exists(path)
        print("file exists")
        
        #todo check table
        
        df = loadDB(path, table)
        assert len(colums) == len(df.columns.values)
        print("columns number is ok")
        
        #check columns
        for index in range(0,len(colums)):
            assert df.columns.values[index] == colums[index]
            print(colums[index] + " is ok")
            
        print("test completed")
    except AssertionError as error:
        print("test failed")
        print(error)
    
def testResults():
    testDB(filePathLocation, "location", ["name", "country", "lat", "lon"])
    testDB(filePathShock, "shockData", ["lat", "lon", "speed", "shock_duration", "x_axis", "y_axis", "z_axis"])

def main():
    deleteFile(filePathLocation)
    deleteFile(filePathShock)
    runAutomatedDataPipeline()
    testResults()
    
    
if __name__ == "__main__":
    main()    
