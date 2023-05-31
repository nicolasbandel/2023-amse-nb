import pandas as pd
import sqlite3
from sqlalchemy.types import Integer, String, Float

shock_url = "data/shockData.sqlite"

columnTypesShock = {'latitude': Float, 
       'longitude': Float,
       'count': Integer
       }

def filter(query, tableName):
    con = sqlite3.connect(shock_url)
    df = pd.read_sql_query(sql=query, con=con)
    df_grouped = df.groupby(['lat', 'lon']).size().reset_index(name='count')
    print(df_grouped)
    df_grouped.to_sql(tableName, 'sqlite:///data/shockData.sqlite', if_exists='replace', index=False)
    con.commit()
    con.close()
    return

#filter standing shock data
filter("SELECT * FROM shockData WHERE speed = 0", 'shockDataStandingFiltered')
#filter moving shock data
filter("SELECT * FROM shockData WHERE speed <> 0", 'shockDataMovingFiltered')

