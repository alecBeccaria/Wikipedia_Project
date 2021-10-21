import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pymongo as db
from pymongo import MongoClient
import re
import codecs
import json

regex = "\[\[.*\(constellation\)\]\]"

def xml_parser(xmlFile):
    wikiDic = {}

    print('Starting Parse')
    xml_root = etree.iterparse(xmlFile, events=('start', 'end'))
    print('Parse Done')


    nsOne = 'http://www.mediawiki.org/xml/export-0.10/'
    ns = {'x': 'http://www.mediawiki.org/xml/export-0.10/'}
    importCusCount = 0
    elementList = []
    for ev, elem in xml_root:
        tag = get_tag_name(elem.tag, elem)
        if ev == 'start':
            if tag == 'page':
                title = ''
                id = -1
                redirect = ''
                inrevison = False
                ns = 0
            elif tag =='title':
                wikiDic['title'] = elem.text
            elif tag == 'text':
                if elem.text != None:
                    match = re.search(regex, elem.text)
                    if match != None:
                        wikiDic['text'] = elem.text
            elif tag == 'id' and not inrevison and elem.text != None:
                id = int(elem.text)
            elif tag == 'redirect':
                redirect = elem.get('title', '')
            elif tag == 'ns' and elem.text != None:
                ns = int(elem.text)
        elem.clear()
    with open('data.txt', 'w') as file:
        json.dump(wikiDic, file)
        file.close()
        wikiDic.clear()



def get_tag_name(tag, elem):
    t = elem.tag
    idx = k = t.rfind('}')
    if idx != -1:
        t = t[idx + 1:]
    return t


def testConn():
    connectionString = 'mongodb+srv://root:tortilla@cluster0.mpjye.mongodb.net/Wookie?retryWrites=true&w=majority'
    client = MongoClient(connectionString)
    server = client._get_server_session()
    database = client.get_database('Wookie')
    collection = database.get_collection('WikiShiz')







if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    testConn()
    #xml_parser('enwiki-latest-pages-articles22.xml-p41496246p42996245')
    # sql_dbms(customers)
