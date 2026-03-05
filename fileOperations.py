import logging

import pandas as pd
import ast
from GlobalConfig import __DATAFRAME__
def _readCSV(feed):
    index_list = []
    for ind in feed['columns'].get('index'):
        index_list.append(int(ind)-1)
    delim = feed['properties'].get("delimiter") if feed['properties'].get("delimiter") is not None else ","
    df = pd.read_csv(feed['feed_name'],usecols=index_list,delimiter=delim,names=feed['columns'].get("name"),dtype=feed['columns'].get('data_type') ,skiprows=int(feed['properties'].get('skipHeader', 0)), skipfooter=int(feed['properties'].get('skipFooter', 0)), engine='python')
    return df

def _writeCSV(df,feedName,delimiter,columnNames,mode):
    try:
        df.to_csv(feedName,sep=delimiter,columns=columnNames, mode=mode ,index=False)
    except KeyError as e:
        print("Column is not defined")
        print(e)
def _writeFixWidth(df,feedName,columnNames,mode):
    with open(feedName, mode) as f:
        f.write(df[columnNames].to_string(index=False))
def _readFixWidth(feed):
    index_list = [ast.literal_eval(i) for i in feed['columns'].get('index')]
    return pd.read_fwf(feed['feed_name'], colspecs=index_list,
                       names=feed['columns'].get("name"),
                       dtype=feed['columns'].get('data_type'), skiprows=int(feed['properties'].get('skipHeader', 0)),
                       skipfooter=int(feed['properties'].get('skipFooter', 0)), engine='python')


def _readDataFrame(name):
    global __DATAFRAME__
    
    if name in __DATAFRAME__:
        return __DATAFRAME__[name]
    else:
        logging.error(f"❌ {name} DataFrame does not exist.")
        return pd.DataFrame()


def _readJSON(feed):
    pass
def _writeJSON(df,feedName,columnNames,mode):
    df[columnNames].to_json(feedName,orient='records',mode=mode,date_format='iso')

def _readPDF(feed):
    pass
def _writePDF(feed):
    pass

def readData(feed):
        feedType = feed['properties'].get('feedType')
        try:
            if feedType == 'CSV' or feedType == "TXT":
                df = _readCSV(feed)
                return df
            elif feedType == "FIXWIDTH":
                df = _readFixWidth(feed)
                return df
            elif feedType == "DATAFRAME".upper():
                df = _readDataFrame(feed['feed_name'])
            else :
                logging.critical("⚠️ Invalid File Format")
                return pd.DataFrame()
        except FileNotFoundError as e:
            logging.critical(e)
            return pd.DataFrame()
        except ValueError as e:
            logging.critical(e)
            return pd.DataFrame()
        except Exception as e:
            logging.critical(e)
            return pd.DataFrame()


def writeData(df,outputFormat):

    feedType = outputFormat['feedType']
    feedName = outputFormat['FeedName']
    delimiter = outputFormat['delimiter']
    columnNames = outputFormat['name']
    mode = outputFormat['mode'].lower()

    if df.empty:
        df = pd.DataFrame(columns=columnNames)
    if feedType == 'CSV' or feedType == "TXT":
        _writeCSV(df, feedName,delimiter,columnNames,mode)
    elif feedType == "FIXWIDTH":
        _writeFixWidth(df, feedName,columnNames,mode)
    elif feedType == "JSON":
        _writeJSON(df, feedName,columnNames,mode)
    else:
        pass
