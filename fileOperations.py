import logging
from lxml import etree
import pandas as pd
import ast
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from GlobalConfig import __DATAFRAME__
def _readCSV(feed):
    index_list = []
    for ind in feed['columns'].get('index'):
        index_list.append(int(ind)-1)
    delim = feed['properties'].get("delimiter") if feed['properties'].get("delimiter") is not None else ","
    df = pd.read_csv(feed['feed_name'],usecols=index_list,delimiter=delim,names=feed['columns'].get("name"),dtype=feed['columns'].get('data_type') ,skiprows=int(feed['properties'].get('skipHeader', 0)), skipfooter=int(feed['properties'].get('skipFooter', 0)), engine='python')
    return df

def _writeCSV(df,feedName,delimiter,columnNames,mode,head):
    try:
        df.to_csv(feedName,sep=delimiter,columns=columnNames, mode=mode, header=head,index=False)
    except KeyError as e:
        print("Column is not defined")
        print(e)
def _writeFixWidth(df,feedName,columnNames,mode,header):
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
def _writeJSON(df,feedName,columnNames,mode,header):
    df[columnNames].to_json(feedName,orient='records',mode=mode,date_format='iso')

def _readPDF(feed):
    pass
def _writePDF(df,pdf_file):

    try:
        with PdfPages(pdf_file) as pdf:
            # Create a figure and axis
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.axis('tight')
            ax.axis('off')

            # Create table from DataFrame
            table = ax.table(
                cellText=df.values,
                colLabels=df.columns,
                cellLoc='center',
                loc='center'
            )

            # Adjust table style
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1.2, 1.2)

            # Save the figure to PDF
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

        print(f"✅ DataFrame successfully saved to '{pdf_file}'")

    except Exception as e:
        print(f"❌ Error: {e}")

def _readXML(feed):
    try:
        # Parse XML
        tree = etree.parse(feed['feed_name'])
        # Extract data using XPath
        dataset={}
        for ind,nm in zip(feed['columns'].get('index'),feed['columns'].get('name')):
            dataset[nm] = tree.xpath(ind)
        # Create DataFrame
        df = pd.DataFrame(dataset)      
        return df
    
    except Exception as e:
        logging.critical(f"⚠️Unexpected error: {e}")

def readData(feed):
        feedType = feed['properties'].get('feedType')
        try:
            if feedType == 'CSV' or feedType == "TXT":
                df = _readCSV(feed)
                return df
            elif feedType == "FIXWIDTH":
                df = _readFixWidth(feed)
                return df
            elif feedType == "XML":
                df = _readXML(feed)
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
    header = outputFormat['header']

    if df.empty:
        df = pd.DataFrame(columns=columnNames)
    if feedType == 'CSV' or feedType == "TXT":
        _writeCSV(df, feedName,delimiter,columnNames,mode,header)
    elif feedType == "FIXWIDTH":
        _writeFixWidth(df, feedName,columnNames,mode,header)
    elif feedType == "JSON":
        _writeJSON(df, feedName,columnNames,mode,header)
    elif feedType == "PDF":
        _writePDF(df,feedName)
    else:
        pass
