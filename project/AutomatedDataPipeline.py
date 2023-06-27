import pandas as pd
import time
from sqlalchemy.types import Integer, String, Float

downloadFiles = False

storage_options = {'User-Agent': 'Mozilla/5.0'}

#latitude,longitude,speed,shock_duration,shock_x_axis,x_axis,shock_y_axis,y_axis,shock_z_axis,z_axis,wagonID,delta_timestamp
usecolsShock = ["latitude", "longitude", "speed", "shock_duration", "x_axis", "y_axis", "z_axis"]
#BST8;BST_NAME;LAND;LAT;LON
usecolsLocation = ["BST_NAME", "LAND", "LAT", "LON"]

columnTypesShock = {'latitude': Float, 
       'longitude': Float,
       'speed': Integer,
       'shock_duration': Integer,
       'x_axis': Float,
       'y_axis': Float,
       'z_axis': Float
       }

columnTypesLocation = {'BST_NAME': String, 
       'LAND': String,
       'LAT': Float,
       'LON': Float
       }

def readShock():
    return pd.read_csv('https://mobilithek.info/mdp-api/files/aux/573487566471229440/ShockData.csv', sep=',', storage_options=storage_options, usecols=usecolsShock)

def readShockLocal():
    return pd.read_csv('C:/Users/nicol/Downloads/ShockData.csv', sep=',', usecols=usecolsShock)

def readLocation():
    return pd.read_csv('https://download-data.deutschebahn.com/static/datasets/betriebsstellen_cargo/GEO_Bahnstellen_EXPORT.csv', sep=';', usecols=usecolsLocation)

def readLoactionLocal():
    return pd.read_csv('C:/Users/nicol/Downloads/GEO_Bahnstellen_EXPORT (2).csv', sep=';', usecols=usecolsLocation)

def attemptRead(numTrys, readfct):
    for x in range(0, numTrys):
        #try:
            return readfct()      
        #except:
        #    sleepTime = 5 + x * 30
        #    print("ERROR: Loading failed " , (x+1) , "/" , numTrys)
        #    if(x+1 == numTrys):
        #        print("Unable to load data")
        #        return None
        #    else:
        #        print("Wait " , sleepTime , "s")
        #        time.sleep(sleepTime)


def automatedDataPipline():
    print("Shocks: STARTED")

    #read the csv file:
    #either download the file or read the local file
    if(downloadFiles):
        df = attemptRead(3, readShock)     
    else:
        df = attemptRead(3, readShockLocal)

    if(df is not None):
        #rename all needed columns:
        df.columns.values[0] = 'lat'
        df.columns.values[1] = 'lon'
        df.columns.values[2] = 'speed'

        #filter by criteria:
        #1: At least one value of x, y or z needs to be above 5 or below -5
        #2. All locations need a latitude and a longitude value
        df = df[(abs(df['x_axis']) > 5.0) | (abs(df['y_axis']) > 5.0 ) | (abs(df['z_axis']) > 5.0)]
        df = df.loc[df['lat'].notnull()]
        df = df.loc[df['lon'].notnull()]

        #save table to sqlite file
        df.to_sql('shockData', 'sqlite:///./data/shockData.sqlite', if_exists='replace', index=False, dtype=columnTypesShock)

        print("Shocks: DONE")
        
    downloadFailed = False
    print("Locations: STARTED")

    #read the csv file:
    #either download the file or read the local file
    if(downloadFiles):
        df = attemptRead(3, readLocation)
    else:
        df = attemptRead(3, readLoactionLocal)

    if(df is not None):
        #rename all needed columns:
        df.columns.values[0] = 'name'
        df.columns.values[1] = 'country'
        df.columns.values[2] = 'lat'
        df.columns.values[3] = 'lon'

        #filter by criteria:
        #1. All locations are in germany
        #2. All locations need a latitude and a longitude value
        df = df.loc[df['country']=="DEUTSCHLAND"]
        df = df.loc[df['lat'].notnull()]
        df = df.loc[df['lon'].notnull()]

        #save table to sqlite file
        df.to_sql('location', 'sqlite:///./data/locationData.sqlite', if_exists='replace', index=False, dtype=columnTypesLocation)

        print("Locations: DONE")
        
def main():
    automatedDataPipline()

if __name__ == "__main__":
    main()