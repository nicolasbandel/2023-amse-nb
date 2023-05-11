import pandas as pd

downloadFiles = False

storage_options = {'User-Agent': 'Mozilla/5.0'}

#latitude,longitude,speed,shock_duration,shock_x_axis,x_axis,shock_y_axis,y_axis,shock_z_axis,z_axis,wagonID,delta_timestamp
if(downloadFiles):
    df = pd.read_csv('https://mobilithek.info/mdp-api/files/aux/573487566471229440/ShockData.csv', sep=',', storage_options=storage_options, nrows=15, usecols=["latitude", "longitude", "speed"])
else:
    df = pd.read_csv('C:/Users/nicol/Downloads/ShockData.csv', sep=',', nrows=15, usecols=["latitude", "longitude", "speed"])

df.columns.values[0] = 'lat'
df.columns.values[1] = 'lon'
df.columns.values[2] = 'speed2'

df.to_sql('shockData', 'sqlite:///2023-amse-nb/data/shockData.sqlite', if_exists='replace', index=False)

print("DONE 1 !")

if(downloadFiles):
    df = pd.read_csv('https://download-data.deutschebahn.com/static/datasets/betriebsstellen_cargo/GEO_Bahnstellen_EXPORT.csv', sep=';', nrows=15)
else:
    df = pd.read_csv('C:/Users/nicol/Downloads/GEO_Bahnstellen_EXPORT (2).csv', sep=',', nrows=15)

df.to_sql('location', 'sqlite:///2023-amse-nb/data/locationData.sqlite', if_exists='replace', index=False)

print("DONE 2 !")