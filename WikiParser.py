import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pymongo as db
import codecs
import pandas as csv

regex = "/Category:([a-zA-Z]+) \(constellation\)/gm"

def xml_parser(xmlFile):
    with open('text.txt', 'w', encoding='utf-8') as file:
        print('Starting Parse')
        xml_root = etree.iterparse(xmlFile, events=('start', 'end'))

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
                elif tag == 'text':
                    text = elem.text
                    if text != None:
                        file.write(text)
                elif tag == 'id' and not inrevison and elem.text != None:
                    id = int(elem.text)
                elif tag == 'redirect':
                    redirect = elem.get('title', '')
                elif tag == 'ns' and elem.text != None:
                    ns = int(elem.text)
            elif tag == 'page':
                print()
            elem.clear()
        file.close()


def get_tag_name(tag, elem):
    t = elem.tag
    idx = k = t.rfind('}')
    if idx != -1:
        t = t[idx + 1:]
    return t


def testConn():
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    connStr = cursor.execute('SELECT 1')
    print(connStr)


if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    xml_parser('wiki.xml-p41496246p42996245')
    # sql_dbms(customers)
