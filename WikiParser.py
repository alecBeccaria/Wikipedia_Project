import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pymongo as db
from pymongo import MongoClient
import re
import codecs
import certifi
import json

constRegex = "\[\[.*\(constellation\)\]\]"
starboxRegex = "^\{\{Starbox begin(.|\n)*?\}\}((.|\n)*?)\{\{Starbox end\}\}"
constellRegex = "(constell = )\[\[(\w*)"


def xml_parser(xmlFile):
    wikiList = []


    print('Starting Parse')
    xml_root = etree.iterparse(xmlFile, events=('start', 'end'))
    print('Parse Done')


    nsOne = 'http://www.mediawiki.org/xml/export-0.10/'
    ns = {'x': 'http://www.mediawiki.org/xml/export-0.10/'}
    importCusCount = 0
    elementList = []
    for ev, elem in xml_root:
        starDic = {}
        tag = get_tag_name(elem.tag, elem)
        if ev == 'start':
            if tag == 'page':
                title = ''
                id = -1
                redirect = ''
                inrevison = False
                ns = 0
            if tag == 'title':
                title = elem.text
            if tag == 'text':
                if elem.text != None:
                    match = re.search(starboxRegex, elem.text)
                    if match != None:
                        starDic['title'] = title
                        print(starDic)
                        # constell = re.search(constellRegex, elem.text)
                        # if constell != None:
                        #     starDic['title'] = title
                        #     starDic['constellation'] = constell.group(2)
                        #     wikiList.append(starDic)
                        #     print(starDic)



        elem.clear()
    with open('data.json', 'w') as file:
        json.dump(wikiList, file)
        file.close()
        print(wikiList)


def get_tag_name(tag, elem):
    t = elem.tag
    idx = k = t.rfind('}')
    if idx != -1:
        t = t[idx + 1:]
    return t


def connDB():
    ca = certifi.where()
    connectionString = 'mongodb+srv://user:bob@cluster0.mpjye.mongodb.net/?retryWrites=true&w=majority'
    client = db.MongoClient(connectionString, tlsCAFILE=ca)
    print(client.server_info())
    connection = client['Wookie']
    server = client._get_server_session()
    database = client.get_database('Wookie')
    collection = connection['WikiShiz']








if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    connDB()
    xml_parser('enwiki-latest-pages-articles22.xml-p41496246p42996245')
    # sql_dbms(customers)
