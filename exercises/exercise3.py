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

useColums = {
    excelToInt('A'): 'date',
    excelToInt('B'): 'CIN',
    excelToInt('C'): 'name',
    excelToInt('M'): 'petrol',
    excelToInt('W'): 'diesel',
    excelToInt('AG'): 'gas',
    excelToInt('AQ'): 'electro',
    excelToInt('BA'): 'hybrid',
    excelToInt('BK'): 'plugInHybrid',
    excelToInt('BU'): 'others',
}

columnTypes = {
    'date': str, 
    'CIN': str, #TODO make custom string with exactly 5 number characters
    'name': str
    }

def main():
    #read and drop first 6 rows
    df = pd.read_csv("https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv", sep=";", encoding='latin1', skiprows=6, usecols=useColums.keys(), names=useColums.values(), dtype=columnTypes)
    
    #delete last 4 rows
    df.drop(df.tail(4).index,inplace=True)

    #df = df[df['CIN'].str.match('^\d{5}$')]
    
    #print(str(df['CIN']))
    
    #set CIN to 5 digit number
    #df = df[df['CIN'].str.contains('^\d{5}$')]
    
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
    
    
    df.to_sql('cars', 'sqlite:///./cars.sqlite', if_exists='replace', index=False)
    
if __name__ == "__main__":
    main()
    print(str(excelToInt('CF')))