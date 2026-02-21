import pandas as pd
def _readCSV(feed):
    return pd.read_csv(feed['feed_name'],usecols=feed['columns'].get('index'),delimiter=feed['properties'].get("delimiter"),names=feed['columns'].get("name"),dtype=feed['columns'].get('data_type') ,skiprows=int(feed['properties'].get('skipHeader', 0)), skipfooter=int(feed['properties'].get('skipFooter', 0)), engine='python')
def _writeCSV(df,feed):
    df.to_csv('output_file.csv', index=False)
def _writeFixWidth(feed):
    pass
def _readFixWidth(feed):
    pass

def _readJSON(feed):
    pass
def _writeJSON(feed):
    pass
def _readFixedWidth(feed):
     print("Not Available")
def _writeFixedWidth(feed):
    pass
def _readPDF(feed):
    pass
def _writePDF(feed):
    pass

def readData(feed):
        print(f"Processing feed: {feed['feed_name']}")
        feedType = feed['properties'].get('feedType')
        if feedType == 'CSV' or feedType == "TXT" or feedType == "FIXWIDTH":
            df = _readCSV(feed)
            return df
        else :
             print("Invalid File Format")

def writeData(df,outputFormat):
    print(f"Processing feed: {outputFormat['feed_name']}")
    feedType = outputFormat['properties'].get('feedType')
