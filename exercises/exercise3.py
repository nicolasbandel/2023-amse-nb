import pandas as pd
import string
from sqlalchemy.types import Integer, String, Float
import numpy as np

def excelToInt(col):
    num = 0
    for c in col:
        if c in string.ascii_letters:   #I have noticed the string libary is not allowed. I could fix this by just hardcoding the numbers
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num - 1

def filterNumberCol(df, col):
    df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
    return df[df[col] > 0]

useColumsNames = [
    'date',
    'CIN',
    'name',
    'petrol',
    'diesel',
    'gas',
    'electro',
    'hybrid',
    'plugInHybrid',
    'others'
]

columnTypes = {
    'date': str, 
    'CIN': str, #TODO make custom string with exactly 5 number characters
    'name': str
    }

columnTypesOut = {'date': String, 
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

def main():
    #read and drop first 6 rows
    df = pd.read_csv("https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv", 
                     sep=";", 
                     encoding='latin1', 
                     skiprows=7, 
                     usecols=[excelToInt('A'),
                              excelToInt('B'),
                              excelToInt('C'),
                              excelToInt('M'),
                              excelToInt('W'),
                              excelToInt('AG'),
                              excelToInt('AQ'),
                              excelToInt('BA'),
                              excelToInt('BK'),
                              excelToInt('BU')], 
                     names=useColumsNames, 
                     dtype=columnTypes)
    
    #delete last 4 rows
    df.drop(df.tail(4).index,inplace=True)

    #match CIN format
    df = df[df['CIN'].str.match('^\d{5}$')]
    
    #filter for negative
    df = filterNumberCol(df, 'petrol')
    df = filterNumberCol(df, 'diesel')
    df = filterNumberCol(df, 'gas')
    df = filterNumberCol(df, 'electro')
    df = filterNumberCol(df, 'hybrid')
    df = filterNumberCol(df, 'plugInHybrid')
    df = filterNumberCol(df, 'others')
    
    df.dropna()
    
    df.to_sql('cars', 'sqlite:///./cars.sqlite', if_exists='replace', index=False, dtype=columnTypesOut)
    
if __name__ == "__main__":
    main()