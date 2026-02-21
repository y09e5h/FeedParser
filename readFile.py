import pandas as pd
def _readCSV(feed):
    return pd.read_csv(feed['feed_name'],usecols=feed['columns'].get('index'),delimiter=feed['properties'].get("delimeter"),names=feed['columns'].get("name"),dtype=feed['columns'].get('data_type') ,skiprows=int(feed['properties'].get('skipHeader', 0)), skipfooter=int(feed['properties'].get('skipFooter', 0)), engine='python')

def _readFixedWidth(feed):
     print("Not Available")
def readData(feed):
        print(f"Processing feed: {feed['feed_name']}")
        feedType = feed['properties'].get('feedType')
        if feedType == 'CSV' or feedType == "TXT":
            df = _readCSV(feed)
            return df
        else :
             print("Invalid File Format")