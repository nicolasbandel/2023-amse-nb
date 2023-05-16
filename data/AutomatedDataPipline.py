import pandas as pd

downloadFiles = True

storage_options = {'User-Agent': 'Mozilla/5.0'}

#latitude,longitude,speed,shock_duration,shock_x_axis,x_axis,shock_y_axis,y_axis,shock_z_axis,z_axis,wagonID,delta_timestamp
usecolsShock = ["latitude", "longitude", "speed", "shock_duration", "x_axis", "y_axis", "z_axis"]
#BST8;BST_NAME;LAND;LAT;LON
usecolsLocation = ["BST_NAME", "LAND", "LAT", "LON"]

print("Shocks: STARTED")

#read the csv file:
#either download the file or read the local file
if(downloadFiles):
    df = pd.read_csv('https://mobilithek.info/mdp-api/files/aux/573487566471229440/ShockData.csv', sep=',', storage_options=storage_options, usecols=usecolsShock)
else:
    df = pd.read_csv('C:/Users/nicol/Downloads/ShockData.csv', sep=',', usecols=usecolsShock)

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
df.to_sql('shockData', 'sqlite:///2023-amse-nb/data/shockData.sqlite', if_exists='replace', index=False)

print("Shocks: DONE")
print("Locations: STARTED")

#read the csv file:
#either download the file or read the local file
if(downloadFiles):
    df = pd.read_csv('https://download-data.deutschebahn.com/static/datasets/betriebsstellen_cargo/GEO_Bahnstellen_EXPORT.csv', sep=';', usecols=usecolsLocation)
else:
    df = pd.read_csv('C:/Users/nicol/Downloads/GEO_Bahnstellen_EXPORT (2).csv', sep=';', usecols=usecolsLocation)

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
df.to_sql('location', 'sqlite:///2023-amse-nb/data/locationData.sqlite', if_exists='replace', index=False)

print("Locations: DONE")