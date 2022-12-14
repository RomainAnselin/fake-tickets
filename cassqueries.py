from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import PreparedStatement
import helper

def simple_test_query(session):
    # Get release
    row = session.execute("select release_version from system.local").one()
    if row:
        print(row[0])
    else:
        print("An error occurred.")

### CREATE TABLE STATEMENTS
def solr_encrypted_tables_create(session):
    for y in range (1,54):
        print ("CREATE TABLE IF NOT EXISTS test.support" + str(i) + "(id int primary key, ownedby text, ticket int, time text)")
        session.execute("CREATE TABLE IF NOT EXISTS test.support" + str(i) +"(id int primary key, ownedby text, ticket int, time text)")
        session.execute("CREATE SEARCH INDEX IF NOT EXISTS ON test.support" + str(i) +" WITH COLUMNS ownedby, ticket AND CONFIG { directoryFactory:'encrypted' };")

def create_table_onepk_gc600(session, ksname):
    tblname='onepk_gc600'
    session.execute("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + """(
        onepk int,
        id int, 
        ownedby text, 
        ticket int, 
        time timestamp, 
        updatedat timestamp, 
        notes text,
        PRIMARY KEY (onepk, id))
        WITH CLUSTERING ORDER BY (id ASC) AND gc_grace_seconds = 600 ;
        """)

### TRUNCATE - USE CAREFULLY !!!
def truncate_table(session, ksname, tblname):
    print("Truncate in progress %s.%s", ksname, tblname)
    session.execute("TRUNCATE TABLE " + ksname + "." + tblname + ";")
