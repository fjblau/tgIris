import pyodbc 
import time
from datetime import datetime
import pyTigerGraph as tg
import json
from rdflib import Graph
import irisnative
    
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
print(authToken)

def rdfTest():
    # Create a Graph
    g = Graph()

    # Parse in an RDF file hosted on the Internet
    g.parse("http://www.w3.org/People/Berners-Lee/card")

    # Loop through each triple in the graph (subj, pred, obj)
    for subj, pred, obj in g:
    # Check if there is at least one triple in the Graph
        if (subj, pred, obj) not in g:
            raise Exception("It better be!")

    # Print the number of "triples" in the Graph
    print(f"Graph g has {len(g)} statements.")
    # Prints: Graph g has 86 statements.

    #  Print out the entire Graph in the RDF Turtle format
    print(g.serialize(format="turtle"))

def set_test_global(iris_native):
    iris_native.set(8888, "^testglobal", "1")
    iris_native.set(9999, "^testglobal", "2")
    global_value1 = iris_native.get("^testglobal", "1")
    global_value2 = iris_native.get("^testglobal", "2")
    print("The value of ^testglobal(1) is {}".format(global_value1))
    print("The value of ^testglobal(2) is {}".format(global_value2))

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

    rdfTest()


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

    cursor = connection.cursor()
    start= datetime.now()

    #iris native connection
    native_connection = irisnative.createConnection(ip, port, namespace, username, password)
    iris_native = irisnative.createIris(native_connection)
    set_test_global(iris_native)

    #Sample select query
    cursor.execute("SELECT * from TG.rdf") 
    row = cursor.fetchone() 
    while row: 
        print(row.s+"->"+row.p+"->"+row.o) 
        
        if(row.p) == 'LIVES_IN':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("city", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "city", row.o, attributes={})
        elif(row.p) == 'WORKS_AT':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("Company", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "Company", row.o, attributes={})
        elif(row.p) == 'WORKS_ON':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("project", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "project", row.o, attributes={})
        elif(row.p) == 'FRIENDS_WITH':
            conn.upsertVertex("person", row.s, attributes=None)
            conn.upsertVertex("person", row.o, attributes=None)
            conn.upsertEdge("person", row.s, row.p, "person", row.o, attributes={})
      

        row = cursor.fetchone()

    end= datetime.now()
    print ("Total elapsed time: ")
    print (end-start)

if __name__ == '__main__':
    run()
    #graph = Graph()
    #graph.parse("https://www.w3.org/People/Berners-Lee/card")
    #list(graph.subjects(1))