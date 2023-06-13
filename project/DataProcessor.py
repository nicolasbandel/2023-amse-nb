import pandas as pd
import sqlite3
from sqlalchemy.types import Integer, String, Float

standing = True #pass as argument

def loadDF(path, table):
    
    query = "SELECT * FROM " + table
    con = sqlite3.connect(path)
    return pd.read_sql_query(sql=query, con=con)
    

def main():
    if(standing):
        shockDF = loadDF("./shockData.sqlite", "shockDataStandingFiltered")
    else:
        shockDF = loadDF("./shockData.sqlite", "shockDataMovingFiltered")
    locationDF = loadDF("./locationData.sqlite", "locationFiltered")
    
    #shockDF.merge(loadDF, left_on='lon') #TODO how to merge on two parameters
    mergedDF = pd.merge(shockDF, locationDF, how='left', left_on=['lon', 'lat'], right_on=['lon', 'lat'])
    print(mergedDF)
    print(str(len(mergedDF['name'].unique())))
    
    print(mergedDF.groupby(['name']).size().reset_index(name='NameCount'))
    
    mergedDF.to_sql('results', 'sqlite:///./results.sqlite', if_exists='replace', index=False)

if __name__ == "__main__":
    main()