import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy.types import Integer, String, Float
import configparser 
import os

shock_url = "./project/data/shockData.sqlite"
shock_urlSplitted = 'sqlite:///./project/data/shockDataSplitted.sqlite'
location_url = "./project/data/locationData.sqlite"
location_urlSplitted = 'sqlite:///./project/data/locationDataSplitted.sqlite'
location_deleteUrl = './project/data/locationDataSplitted.sqlite'
shock_deleteUrl = './project/data/shockDataSplitted.sqlite'

columnTypesShock = {'latitude': Float, 
       'longitude': Float,
       'count': Integer
       }

def loadConfig():
    config = configparser.ConfigParser()
    config.read_file(open(r'./project/Config.txt'))
    global lonStart 
    global lonEnd
    global latStart
    global latEnd
    global numGeoSteps
    lonStart= int(config.get('Geo Data', 'lonStart'))
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
        
def update_progress(progress):
    print('\r[{0}] {1}%'.format('#'*int(progress/10), round(progress)), end='')

def createGeoDataTable(df, latFrom, latTo, lonFrom, lonTo, tableName, tableUrl, counter):
    #filter data
    df = df[df.lat >= latFrom]
    df = df[df.lat <= latTo]
    df = df[df.lon >= lonFrom]
    df = df[df.lon <= lonTo]
    
    #write date if any left
    if not df.empty:
        df.to_sql(tableName + "_" + str(counter), tableUrl, if_exists='replace', index=False)

def customRange(start, end, step):
    while start <= end:
        yield start
        start += step

def filterShockData(query, tableName):
    con = sqlite3.connect(shock_url)
    df = pd.read_sql_query(sql=query, con=con)
    df_grouped = df.groupby(['lat', 'lon']).size().reset_index(name='count')
    
    lonStep = (lonEnd - lonStart) / numGeoSteps
    latStep = (latEnd - latStart) / numGeoSteps
    counter = 0
    for latFrom in customRange(latStart, latEnd, latStep):
        update_progress((latFrom/latEnd)*100)
        for lonFrom in customRange(lonStart, lonEnd, lonStep):
            createGeoDataTable(df_grouped, latFrom, latFrom+latStep, lonFrom, lonFrom+lonStep, tableName, shock_urlSplitted, counter)
            counter += 1
    con.commit()
    con.close()
    return

def filterLocationData(tableName):
    con = sqlite3.connect(location_url)
    df = pd.read_sql_query(sql="SELECT * FROM location", con=con)
    
    #group by same name
    df = df.groupby(by=['name', 'country']).mean().reset_index()
    df = df.groupby(by=['lat', 'lon']).agg({'name':'first', 'country':'first'}).reset_index()
    
    lonStep = (lonEnd - lonStart) / numGeoSteps
    latStep = (latEnd - latStart) / numGeoSteps
    counter = 0
    for latFrom in customRange(latStart, latEnd, latStep):     
        update_progress((latFrom/latEnd)*100)
        for lonFrom in customRange(lonStart, lonEnd, lonStep):
            createGeoDataTable(df, latFrom, latFrom+latStep, lonFrom, lonFrom+lonStep, tableName, location_urlSplitted, counter)
            counter += 1
    con.commit()
    con.close()

def main():
    #delete old files
    deleteFile(shock_deleteUrl)
    deleteFile(location_deleteUrl)
    #load Config
    loadConfig()
    #filter standing shock data
    print("filter shock data standing:")
    filterShockData("SELECT * FROM shockData WHERE speed = 0", 'shockDataStandingFiltered')
    print("\rDone!                                                  ")
    #filter moving shock data
    print("filter shock data moving:")
    filterShockData("SELECT * FROM shockData WHERE speed <> 0", 'shockDataMovingFiltered')
    print("\rDone!                                                  ")
    #round location data
    print("filter location data:")
    filterLocationData('locationFiltered')
    print("\rDone!                                                  ")

if __name__ == "__main__":
    main()