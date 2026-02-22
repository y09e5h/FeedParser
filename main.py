import pandas as pd
import xml.etree.ElementTree as ET
import numpy as np
from fileOperations import readData, writeData
from discard import applyDiscard
from enrichment import applyEnrichment
from extractStatic import staticGenerator
from GlobalConfig import STATIC_VARIABLES

class FeedParser:
    def __init__(self, xml_file):
        self.xml_file = xml_file
        self.feeds = []
        self.parse_xml()

    def parse_xml(self):
        tree = ET.parse(self.xml_file)
        root = tree.getroot()
        
        for feed in root.findall('Feed'):
            feed_info = {
                'feed_name': feed.get('FeedName'),
                'properties': {},
                'columns': {},
                'staticColumn':[],
                'discards': [],
                'enrichment': [],
                'single_stage_discard': []
            }
            
            properties = feed.find('properties')
            if properties is not None:
                feed_info['properties'] = properties.attrib
            
            columns = feed.find('columns')
            if columns is not None:
                cilist=[]
                nmlist=[]
                dtype={}
                for column in columns.findall('column'):
                    cilist.append(int(column.get('index'))-1)
                    nmlist.append(column.text)
                    dtype[column.text]=column.get('DataType')

                feed_info['columns']= {
                    'index':cilist,
                    'data_type': dtype,
                    'name': nmlist
                }
            
            discards = feed.find("discards")
            for rule in discards:
                feed_info['discards'].append(rule.text)

            
            staticColumns = feed.find('staticColumns')
            
            for staticColumn in staticColumns:
                feed_info['staticColumn'].append({
                    'column' : staticColumn.get("name"),
                    'filter': (staticColumn.find("filter")).text,
                    'rowNumber': (staticColumn.find("filter")).get("rowNumber",""),
                    'resultIndex': (staticColumn.find("filter")).get("resultIndex","0"),
                    'rule' : (staticColumn.find("rule")).text,
                    'dataType': "str"
                })



            enrichemnts = feed.find('Enrichments')
            for enrichment in enrichemnts:
                feed_info['enrichment'].append({
                    'column' : enrichment.get("ColumnName"),
                    'filter': (enrichment.find("filter")).text if enrichment.find("filter") is not None else "" ,
                    'rule' : (enrichment.find("rule")).text,
                    'dataType': enrichment.get("dataType","")
                })
            output = feed.find('output')
            if output is not None:
                nmlist = []
                for column in output.findall('column'):
                    nmlist.append(column.text)

                feed_info['output'] = {
                    'FeedName' : output.get("FeedName"),
                    'delimiter' : output.get("delimiter"),
                    'feedType' : output.get("feedType"),
                    'name': nmlist,
                    'mode' : output.get("mode")
                }

            rules = feed.find('SingleStageDiscard')
            if rules is not None:
                for rule in rules:
                    feed_info['single_stage_discard'].append(rule.text)
            
            self.feeds.append(feed_info)


            
if __name__ == "__main__":
    parser = FeedParser('TestFile.xml')
    print(parser.feeds)
    for feed in parser.feeds:
        df = readData(feed)
        staticGenerator(feed["feed_name"],feed['properties'].get('feedType'),feed["staticColumn"])
        df = applyDiscard(df,feed["discards"])
        df = applyEnrichment(df,feed["enrichment"])
        df = applyDiscard(df,feed["single_stage_discard"])
        #writeData(df,feed["output"])
        print(df)
        df.info()
