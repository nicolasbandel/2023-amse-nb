import pandas as pd
import sqlite3
from sqlalchemy.types import Integer, String, Float
from math import radians, cos, sin, asin, sqrt
import configparser 
import re
import os

ALL_DATA = 0
STANDING_DATA = 1
MOVING_DATA = 2

shock_url = "./project/data/shockDataSplitted.sqlite"
location_url = "./project/data/locationDataSplitted.sqlite"
result_url = "./project/data/data.sqlite"
radius = 1

def loadConfig():
    config = configparser.ConfigParser()
    config.read_file(open(r'./project/Config.txt'))
    global lonStart
    global lonEnd
    global latStart
    global latEnd
    global numGeoSteps
    lonStart = int(config.get('Geo Data', 'lonStart'))
    lonEnd = int(config.get('Geo Data', 'lonEnd'))
    latStart = int(config.get('Geo Data', 'latStart'))
    latEnd = int(config.get('Geo Data', 'latEnd'))
    numGeoSteps = int(config.get('Geo Data', 'numGeoSteps'))
    
def deleteFile(path):
    if os.path.exists(path):
        os.remove(path)
        print("Successfully! The File has been removed")
    else:
        print("Can not delete the file as it doesn't exists")
    
def customRange(start, end, step):
    while start <= end:
        yield start
        start += step    
        
def getTableEnding(name):
    m = re.search(r"locationFiltered([^\n\r]*)", name)
    return m.group(1)

def mergeWithTableList(locationTableName, shockTableName, shockTables, resultTable):
    if (shockTableName,) in shockTables:
            #load both tables 
            dfLoc =loadDF(location_url, locationTableName)
            dfShock = loadDF(shock_url, shockTableName)
            #cross merge Tables
            ##create merge column
            dfLoc['merge'] = 1
            dfShock['merge'] = 1
            dfMerged = pd.merge(dfLoc, dfShock, on='merge')
            
            #calculate distance
            dfMerged['distance'] = dfMerged.apply(lambda row: haversine(row['lon_x'], row['lat_x'], row['lon_y'], row['lat_y']), axis=1)
            
            #remove all with a distance smaller than 1000km
            dfMerged = dfMerged[dfMerged.distance < radius]
            
            dfMerged = dfMerged.groupby(by=['name', 'country', 'lat_x', 'lon_x'])['count'].sum().reset_index()
            dfMerged.to_sql(resultTable, 'sqlite:///./project/data/data.sqlite', if_exists='append', index=False)
        
def createResultTableName(mergeType):
    if(mergeType == ALL_DATA):
        return "resultTable_All"    
    if(mergeType == STANDING_DATA):
        return "resultTable_Standing"
    if(mergeType == MOVING_DATA):
        return "resultTable_Moving"   
    
def update_progress(progress):
    print('\r[{0}] {1}%'.format('#'*int(progress/10), round(progress)), end='')
    
def mergeTables(mergeType):
    
    locationTables = getAllTables(location_url)
    shockTables = getAllTables(shock_url)
    totalNum = len(locationTables)
    count = 0
    for locationTable in locationTables:
        update_progress(float((count/totalNum)*100))
        #check if shock table exists
        locationTableName = locationTable[0]
        if(mergeType == ALL_DATA or mergeType == STANDING_DATA):
            shockTableName = "shockDataStandingFiltered" + str(getTableEnding(locationTableName))
            mergeWithTableList(locationTableName, shockTableName, shockTables, createResultTableName(mergeType))
        if(mergeType == ALL_DATA or mergeType == MOVING_DATA):
            shockTableName = "shockDataMovingFiltered" + str(getTableEnding(locationTableName))
            mergeWithTableList(locationTableName, shockTableName, shockTables, createResultTableName(mergeType))
        count = count + 1

def loadDF(path, table):
    
    query = "SELECT * FROM " + table #+ " LIMIT 100"
    con = sqlite3.connect(path)
    df = pd.read_sql_query(sql=query, con=con)
    return df
        
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def getAllTables(url):
    try:
        con = sqlite3.connect(url)
        # Getting all tables from sqlite_master
        sql_query = """SELECT name FROM sqlite_master
        WHERE type='table';"""
        cursor = con.cursor()
        cursor.execute(sql_query)
        
        #return list of tables
        return cursor.fetchall()
    
    except sqlite3.Error as error:
        print("Failed to execute the above query", error)
        
    finally:
        if con:
            con.close()

def processTable(table):
    df = pd.read_sql_table(table, 'sqlite:///project/data/data.sqlite')
    dfMerged = pd.merge(df, df, on='count')
    dfMerged['distance'] = dfMerged.apply(lambda row: haversine(row['lon_x_x'], row['lat_x_x'], row['lon_x_y'], row['lat_x_y']), axis=1)     
    #remove all with a distance smaller than 1000km
    dfMerged = dfMerged[dfMerged.distance < (radius/2)]
    #dfMerged.groupby(by={'count'}, )
    print(dfMerged)

def processResults():
    dfAll_Full = pd.read_sql_table('resultTable_All', 'sqlite:///project/data/data.sqlite', columns=['name','country','lat_x','lon_x','count'])
    processTable('resultTable_All')
    dfStanding_Full = pd.read_sql_table('resultTable_Standing', 'sqlite:///project/data/data.sqlite')
    dfMoving_Full = pd.read_sql_table('resultTable_Moving', 'sqlite:///project/data/data.sqlite')

    dfAll_Full = dfAll_Full.groupby(by=['name', 'country']).agg({ 'lat_x':'first', 'lon_x':'first','count':'sum'}).reset_index()

def main():
    print("process Data with radius " + str(radius) + ":")
    #delete old files
    deleteFile(result_url)
    
    #load Config
    loadConfig()
    
    #merge Tables
    print("process all Data:")
    mergeTables(ALL_DATA)
    print("\rDone!                                                  ")
    print("process standing Data:")
    mergeTables(STANDING_DATA)
    print("\rDone!                                                  ")
    print("process moving Data:")
    mergeTables(MOVING_DATA)
    print("\rDone!                                                  ")
    #processResults()


if __name__ == "__main__":
    main()