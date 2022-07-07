# Romain Anselin - 2022
import random
import os
import sys
import re
import helper
from datetime import datetime, timedelta
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import PreparedStatement
from cassandra.auth import PlainTextAuthProvider

# Check arguments
# (note 2 includes arg 0 which is this script!)
if len(sys.argv) == 1:
    # Default values if none set
    startval=0
    numrec=1000000
elif len(sys.argv) == 3:
    print("debug arg0 ",sys.argv[0]," arg1 ",sys.argv[1]," arg2 ",sys.argv[2],"\n")
    startval=int(sys.argv[1])
    numrec=int(sys.argv[2])
elif len(sys.argv) != 3:
    print(
            "\n***",sys.argv[0], "***\n"
            'Incorrect number of arguments, please run script as follows:'
            '\n\n'+str(sys.argv[0])+' <nothing if you want to use default values>'
            '\n\n'+str(sys.argv[0])+' <start value> <number of records to insert>'
        )
    sys.exit(0)

### Astra conf
config = helper.read_config()
isAstra = config['general']['isAstra']
dcname = config['general']['dcname']
ksname = config['general']['ksname']
tblname = config['general']['tblname']

if isAstra == "true":
    scb = config['astra']['scb']
    token = config['astra']['token']
    secret = config['astra']['secret']
    #print("SCB: " + scb + "\ntoken: " + token + "\nsecret: " + secret)

    cloud_config= {
        'secure_connect_bundle': scb
    }
    auth_provider = PlainTextAuthProvider(token, secret)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
else:
    # Internal cluster
    host1 = config['hosts']['host1']
    host2 = config['hosts']['host2']
    authenabled = config['hosts']['isauthenabled']
    if authenabled == "true":
        my_user = config['hosts']['username']
        my_pwd = config['hosts']['password']
        auth_provider = PlainTextAuthProvider(username=my_user, password=my_pwd)
        cluster = Cluster([host1, host2], auth_provider=auth_provider)
    else:
        cluster = Cluster([host1,host2])

session = cluster.connect()

# Get release
row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")

### Values implementation
endval=startval+numrec
owner = [ "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan", "Alberto", "Peter", "Pav", "Alkesh", "Uzoma", "Calum", "Cordell"]
power10 = [1,10,100,1000,10000,100000,1000000,10000000]

### Starting routine

### Create multiple tables + Solr encrypted core !!! Use with Caution as per DSP-
'''
for y in range (1,54):
    print ("CREATE TABLE IF NOT EXISTS test.support" + str(i) +"(id int primary key, ownedby text, ticket int, time text)")
    session.execute("CREATE TABLE IF NOT EXISTS test.support" + str(i) +"(id int primary key, ownedby text, ticket int, time text)")
    #session.execute("CREATE SEARCH INDEX IF NOT EXISTS ON test.support" + str(i) +" WITH COLUMNS ownedby, ticket AND CONFIG { directoryFactory:'encrypted' };")
'''

# Create ks/table
if isAstra == "false":
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksname + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + dcname + "': '2'}")

session.execute("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + """(
    id int, 
    ownedby text, 
    ticket int, 
    time timestamp, 
    updatedat timestamp, 
    notes text,
    PRIMARY KEY (id))""")
#print("Truncate in progress")
#session.execute("TRUNCATE TABLE " + ksname + "." + tblname + ";")

# Create SAI
# !!! TIL: Originally, index called dept_sai_idx hardcoded. It will not create the index as it exists for another table, but it will not error out either !!!
session.execute("CREATE SEARCH INDEX IF NOT EXISTS ON " + ksname + "." + tblname + " WITH COLUMNS ownedby, updatedat, time, notes")

'''try:
    session.execute("CREATE CUSTOM INDEX IF NOT EXISTS " + tblname + "_sai_idx ON " + ksname + "." + tblname + "(time) USING 'StorageAttachedIndex'")
except:
    print ("ERROR: Unable to create index:\n" "CREATE CUSTOM INDEX IF NOT EXISTS " + tblname + "_sai_idx ON " + ksname + "." + tblname + "(time) USING 'StorageAttachedIndex'")
'''

# Insert Bind
ticket_insert = "INSERT INTO " + ksname + "." + tblname + "(id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?)"
ticketins_prep = session.prepare(ticket_insert, ConsistencyLevel.QUORUM)
# Select Bind
ticket_select = """
    SELECT id,  ownedby, ticket, time, notes
    FROM """ + ksname + "." + tblname + """
    WHERE id = ?
    """
ticketsel_prep = session.prepare(ticket_select, ConsistencyLevel.QUORUM)

ticket_sel_count = """
    SELECT count(*)
    FROM """ + ksname + "." + tblname + """
    WHERE id = ?
    """
ticketselcnt_prep = session.prepare(ticket_sel_count, ConsistencyLevel.QUORUM)

for myrange in range (1,2):
    # min_time = datetime.now()
    min_time = helper.current_milli_time()

    for i in range (startval,endval):
        # timenow = datetime.now()
        timenow = helper.current_milli_time()
        pickowner = str(random.choice(owner))
        pickticket = random.randint(0,100000)
        if i%2 == 0:
            datastr = 'Python writing data with value ' + str(i) + ' for ' + pickowner + ' at ' + str(helper.fromts(timenow))
        else:
            datastr = 'Checking Solr by sending different data'

        ### Bound statement
        tickins_bind = ticketins_prep.bind((i, pickowner, pickticket, timenow, datastr))
        session.execute(tickins_bind)

        # Show time for every 10k records
        if i%10000 == 0:
            print( "Written " + str(i) + " records so far, time now " + str(helper.dtn()) )

        # Read every power of 10 the content
        # if (helper.isPower10(i,10)):
        if i in power10:
            print( "Record " + str(i) + " reading now " + str( helper.dtn() ) )
            startselread = helper.dtn()
            ### READ SCENARIO
            ticksel_bind = ticketsel_prep.bind([i])
            selrows = session.execute(ticksel_bind)
            
            for row in selrows:
                print(row.id, row.ownedby, row.ticket, row.time, row.notes)
            
            tickselcnt_bind = ticketselcnt_prep.bind([i])
            cntrows = session.execute(ticksel_bind)
            for row in selrows:
                print("Number of rows: " + str(row.count))

            print( "Read execution finished: " + str( helper.dtn() - startselread ) )
            
            '''
            saiSimplestmt = SimpleStatement( "SELECT id,  ownedby, ticket, time, notes FROM " + ksname + "." + tblname + " WHERE time >= %s AND time <= %s;" % (min_time, timenow) ,
               consistency_level=ConsistencyLevel.QUORUM)

            startsairead = helper.dtn()

            sai_rows = session.execute(saiSimplestmt)
            
            for sai_row in sai_rows:
                print(sai_row.id, sai_row.ownedby, sai_row.ticket, sai_row.time, sai_row.notes)
            print( "SAI READ " + str(i) + " FINISHED " + str( helper.dtn() - startsairead ) )
            '''

            # saicntSimplestmt = SimpleStatement( "select count(*) FROM " + ksname + "." + tblname + " WHERE my_id = %s AND time >= %s AND time <= %s;" % (i, min_time, timenow) ,
            #     consistency_level=ConsistencyLevel.QUORUM)
            # saicnt_rows = session.execute(saicntSimplestmt)
            # for saicnt_row in saicnt_rows:
            #   print("Number of SAI rows: " + str(sai_row.count))
            # saicnt_rows = session.execute(saicntSimplestmt)