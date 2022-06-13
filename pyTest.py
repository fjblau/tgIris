import pyodbc 
import time
from datetime import datetime
import pyTigerGraph as tg
import json

CONFIG = json.load(open('config.json'))
#print(CONFIG)

conn = tg.TigerGraphConnection(
    host=CONFIG['host'],
    graphname=CONFIG['graph'],
    username=CONFIG['username'],
    password=CONFIG['password']
)

conn.graphname="pyTest"
authToken = conn.getToken(CONFIG['secret'], setToken=True, lifetime=10000)


def get_connection_info(file_name):
    # Initial empty dictionary to store connection details
    connections = {}

    # Open config file to get connection info
    with open(file_name) as f:
        lines = f.readlines()
        for line in lines:
            # remove all white space (space, tab, new line)
            line = ''.join(line.split())

            # get connection info
            connection_param, connection_value = line.split(":")
            connections[connection_param] = connection_value
    return connections

def run():
    connection_detail = get_connection_info("connection.config")

    ip = connection_detail["ip"]
    port = int(connection_detail["port"])
    namespace = connection_detail["namespace"]
    username = connection_detail["username"]
    password = connection_detail["password"]
    driver = "{InterSystems ODBC}"

    # Create connection to InterSystems IRIS
    connection_string = 'DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}'\
        .format(driver, ip, port, namespace, username, password)
    connection = pyodbc.connect(connection_string)

    connection = pyodbc.connect(connection_string)
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')
    print("Connected to InterSystems IRIS")

    lowptr=connection.getinfo(127)
    highptr=connection.getinfo(136)
    #value = PyLong_FromUnsignedLongLong(lowptr)
    #print("%#5.8x"% (value))

    print ("Connection high pointer: ")
    print (format(highptr, '02x'))
    print ("Connection high pointer: ")
    print("%#5.8x"% (highptr))
    print ("Connection low pointer: ")
    print("%#5.8x"% (lowptr))
    cursor = connection.cursor()
    start= datetime.now()


    #Sample select query
    cursor.execute("SELECT * from TG.rdf") 
    row = cursor.fetchone() 
    while row: 
        print(row.s+"->"+row.p+"->"+row.o) 
        
        if(row.p) == 'LIVES_IN':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("city", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "city", row.o, attributes={})
        if(row.p) == 'WORKS_AT':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("Company", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "Company", row.o, attributes={})

        row = cursor.fetchone()




    end= datetime.now()
    print ("Total elapsed time: ")
    print (end-start)

if __name__ == '__main__':
    run()
