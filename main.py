import xml.etree.ElementTree as ET
from fileOperations import readData, writeData
from discard import applyDiscard
from enrichment import applyEnrichment
from extractStatic import staticGenerator
from outputGenerator import getOutput
from lxml import etree
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

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
                'single_stage_discard': [],
                'outputs':[]
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
                    cilist.append(column.get('index'))
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
            if staticColumns is not None:
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
            if enrichemnts is not None:
                for enrichment in enrichemnts:
                    etype = 'RECORD'
                    if enrichment.tag == 'GroupEnrichment':
                        etype = "GROUP"
                    elif enrichment.tag == 'JoinEnrichment':
                        etype = "JOIN"
                    else:
                        etype = "RECORD"

                    if etype != "JOIN":
                        feed_info['enrichment'].append({
                            'type' : etype,
                            'column' : enrichment.get("ColumnName"),
                            'filter': (enrichment.find("filter")).text if enrichment.find("filter") is not None else "" ,
                            'rule' : (enrichment.find("rule")).text,
                            'groupBy' : (enrichment.find('groupBy')).text if enrichment.find('groupBy') is not None else "",
                            'dataType': enrichment.get("dataType","")
                        })
                    else:
                        feed_info['enrichment'].append({
                            'type' : etype,
                            'DataFrame' : enrichment.get("DataFrame"),
                            'on': (enrichment.find("on")).text,
                            'how' : (enrichment.find("how")).text
                        })

            outputs = feed.find('outputs')
            if outputs is not None:
                for output in outputs:
                    #print(output)
                    nmlist = []
                    for column in output.find('columns'):
                        #print("1----->")
                        #print(column)
                        nmlist.append(column.text)

                    feed_info['outputs'].append({
                        'FeedName' : output.get("FeedName"),
                        'delimiter' : output.get("delimiter"),
                        'feedType' : output.get("feedType"),
                        'name': nmlist,
                        'mode' : output.get("mode") if output.get("mode") is not None else "W",
                        'header' : output.get("header") if output.get("header") is not None else "True",
                        'filter' : (output.find("filter")).text if output.find("filter") is not None else "" 
                    })

            rules = feed.find('SingleStageDiscard')
            if rules is not None:
                for rule in rules:
                    feed_info['single_stage_discard'].append(rule.text)
            
            self.feeds.append(feed_info)

def validate_xml(xml_file, xsd_file):
    try:
        # Load XSD
        with open(xsd_file, 'rb') as f:
            schema_root = etree.XML(f.read())
            schema = etree.XMLSchema(schema_root)

        # Load XML
        with open(xml_file, 'rb') as f:
            xml_doc = etree.XML(f.read())

        # Validate
        schema.assertValid(xml_doc)

        logging.info("✅ XML is valid!")
        return True

    except etree.DocumentInvalid as e:
        logging.critical("❌ XML is invalid!")
        logging.critical(e)
        return False

    except Exception as e:
        logging.critical("⚠️ Error:", e)
        return False
            
if __name__ == "__main__":
    configXmlFile = "C:\\Users\\yogesh.patil\\Desktop\\FeedParser\\TestFile.xml"
    configXsd="C:\\Users\\yogesh.patil\\Desktop\\FeedParser\\validator.xsd"
    logging.info(f"Validating {configXmlFile}")
    if not validate_xml(configXmlFile, configXsd):
        exit(1)
    logging.info(f"Parsing {configXsd}")
    parser = FeedParser(configXmlFile)
    print(parser.feeds)
    for feed in parser.feeds:
        logging.info("")
        logging.info(f"================================ START {feed['feed_name']}===========================")
        logging.info(f"Reading data from {feed['feed_name']}")
        df = readData(feed)
        if not df.empty:
            if feed["staticColumn"] is not None:
                logging.info(f"Generating Static data")
                staticGenerator(feed["feed_name"],feed['properties'].get('feedType'),feed["staticColumn"])
            if feed["discards"] is not None:
                logging.info(f"Applying Discard")
                df = applyDiscard(df,feed["discards"])
            if feed["enrichment"] is not None:
                if not df.empty:
                    logging.info(f"Applying Enrichment")
                    df = applyEnrichment(df,feed["enrichment"])
                else:
                    logging.warning(f"Empty dataset skipping the Enrichment")
            if feed["single_stage_discard"] is not None:
                if not df.empty:
                    logging.info(f"Applying Stage Discard")
                    df = applyDiscard(df,feed["single_stage_discard"])
                else:
                    logging.warning(f"Empty dataset skipping the Stage Discard")
        else:
            logging.warning(f"Processing skipped : Data is empty for {feed['feed_name']}")
        
        for output in feed["outputs"]:
            logging.info(f"Writing to {output['FeedName']}")
            writeData(getOutput(df,output),output)

