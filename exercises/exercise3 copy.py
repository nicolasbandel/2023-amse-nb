import pandas as pd
import string
from sqlalchemy.types import Integer, String, Float
import numpy as np

def excelToInt(col):
    num = 0
    for c in col:
        if c in string.ascii_letters:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num - 1

def cinParser(col):
    lengths = df.apply(lambda x: len(x[0]))
    mask = lengths < 5
    if any(mask in col):
        print(col + "not 5 long")
        
    return "x"

def filterNumberCol(df, col):
    df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
    return df[df[col] > 0]

def main():
    #read and drop first 6 rows
    df = pd.read_csv("https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv", sep=";", encoding='latin1', skiprows=6)
    
    #select needed columns
    df = df.iloc[:,[excelToInt('A'),excelToInt('B'),excelToInt('C'),excelToInt('M'),excelToInt('W'),excelToInt('AG')
                    ,excelToInt('AQ'),excelToInt('BA'),excelToInt('BK'),excelToInt('BU')]]
    
    #delete last 4 rows
    df.drop(df.tail(4).index,inplace=True)
    
    #df = df.rename(mapper=cinParser, columns='CIN', axis='columns')

    print(str(len(df.columns)) + "new len")

    #rename cols
    df = df.rename(columns={df.columns[0]: 'date'})
    df = df.rename(columns={df.columns[1]: 'CIN'})
    df = df.rename(columns={df.columns[2]: 'name'})
    df = df.rename(columns={df.columns[3]: 'petrol'})
    df = df.rename(columns={df.columns[4]: 'diesel'})
    df = df.rename(columns={df.columns[5]: 'gas'})
    df = df.rename(columns={df.columns[6]: 'electro'})
    df = df.rename(columns={df.columns[7]: 'hybrid'})
    df = df.rename(columns={df.columns[8]: 'plugInHybrid'})
    df = df.rename(columns={df.columns[9]: 'others'})

    df['CIN'] = df['CIN'].astype(str)
    
    print(str(df['CIN']))
    
    #set CIN to 5 digit number
    #df = df[df['CIN'].str.match('^\d{5}$')]
    df = df[df['CIN'].str.contains('^\d{5}$')]
    
    #filter for negative
    df = filterNumberCol(df, 'petrol')
    df = filterNumberCol(df, 'diesel')
    df = filterNumberCol(df, 'gas')
    df = filterNumberCol(df, 'electro')
    df = filterNumberCol(df, 'hybrid')
    df = filterNumberCol(df, 'plugInHybrid')
    df = filterNumberCol(df, 'others')
    
    df.dropna()
    
    #remove all values >= 0
    #df[df.select_dtypes(include=[np.number]).ge(1).all(1)]
    
    columnTypes = {'date': String, 
       'CIN': String, #TODO make custom string with exactly 5 number characters
       'name': String,
       'petrol': Integer, #TODO make > 0
       'diesel': Integer, #TODO make > 0,
       'gas': Integer, #TODO make > 0,
       'electro': Integer, #TODO make > 0
       'hybrid': Integer, #TODO make > 0
       'plugInHybrid': Integer, #TODO make > 0
       'others': Integer, #TODO make > 0
       }
    
    
    df.to_sql('cars', 'sqlite:///./cars.sqlite', if_exists='replace', index=False, dtype=columnTypes)
    
if __name__ == "__main__":
    main()
    print(str(excelToInt('CF')))