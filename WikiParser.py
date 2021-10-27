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
starboxRegex = "\{\{[s|S]tarbox begin([.|\s]*.*)[\n]?\}\}((.|\n)*?)\{\{[s|S]tarbox end\}\}"
constellRegex = "(constell[\s=]*)\[\[(\w*)"
massRegex = "[M|m]ass\s*=\s*(\s*{{[v|V]al\|)*(\d*\.*\d*)"
radiusRegex = "[r|R]adius\s*=\s*({{[v|V]al\|)*(\d*\.*\d*)"
temperatureRegex = "temperature\s*=\s*({{V*|v*al\|)*(\d*,*\d*)"

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
                        constell = re.search(constellRegex, elem.text)
                        mass = re.search(massRegex, elem.text)
                        radius = re.search(radiusRegex, elem.text)
                        temperature = re.search(temperatureRegex, elem.text)
                        starDic['title'] = title


                        isGood = True
                        if constell and mass and radius and temperature != None:
                        # print(title)
                        # print("\n====================\n"+elem.text+"\n=======================\n")

                            starDic['constellation'] = constell.group(2)
                            if mass.group(2) != '' or ' ' or None:
                                #print(elem.text)
                                starDic['mass'] = mass.group(2)
                                if starDic['mass'] == '':
                                    starDic['mass'] = "Not Given"
                                print(starDic['mass'])

                            if radius.group(2) != "" or None:
                                starDic['radius'] = radius.group(2)
                            else:
                                starDic['radius'] = "Not Given"

                            if temperature.group(2) != '' or None:
                                starDic['temperature'] = temperature.group(2)
                            else:
                                starDic['temperature'] = 'Not Given'
                            wikiList.append(starDic)





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



def importJSON():
    ca = certifi.where()
    connectionString = 'mongodb+srv://user:bob@cluster0.mpjye.mongodb.net/?retryWrites=true&w=majority'
    client = db.MongoClient(connectionString, tlsCAFILE=ca)
    connection = client['Wookie']
    server = client._get_server_session()
    database = client.get_database('Wookie')
    collection = connection['WikiShiz']
    with open('data.json') as data:
        file_data = json.load(data)
    if isinstance(file_data, list):
        collection.insert_many(file_data)
    else:
        collection.insert_one(file_data)




if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    connDB()
    #xml_parser('Wiki2.xml')
    importJSON()
    # sql_dbms(customers)
