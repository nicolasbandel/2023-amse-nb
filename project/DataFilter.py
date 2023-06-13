import pandas as pd
import sqlite3
from sqlalchemy.types import Integer, String, Float

shock_url = "./shockData.sqlite"
location_url = "./locationData.sqlite"
debugRoundingRoughtness = 3 #TODO this should be improved. Solution set a radius so the lon +- the radius is ok

columnTypesShock = {'latitude': Float, 
       'longitude': Float,
       'count': Integer
       }

def filterShockData(query, tableName):
    con = sqlite3.connect(shock_url)
    df = pd.read_sql_query(sql=query, con=con)
    df_grouped = df.groupby(['lat', 'lon']).size().reset_index(name='count')
    #print(df_grouped)
    #TODO round data to get a match
    df_grouped = df_grouped.round({'lon':debugRoundingRoughtness, 'lat':debugRoundingRoughtness})
    df_grouped.to_sql(tableName, 'sqlite:///./shockData.sqlite', if_exists='replace', index=False)
    con.commit()
    con.close()
    return

def filterLocationData(tableName):
    con = sqlite3.connect(location_url)
    df = pd.read_sql_query(sql="SELECT * FROM location", con=con)
    df = df.round({'lon':debugRoundingRoughtness, 'lat':debugRoundingRoughtness})
    df.to_sql(tableName, 'sqlite:///./locationData.sqlite', if_exists='replace', index=False)
    con.commit()
    con.close()

def main():
    #filter standing shock data
    filterShockData("SELECT * FROM shockData WHERE speed = 0", 'shockDataStandingFiltered')
    #filter moving shock data
    filterShockData("SELECT * FROM shockData WHERE speed <> 0", 'shockDataMovingFiltered')
    #round location data
    filterLocationData('locationFiltered')

if __name__ == "__main__":
    main()