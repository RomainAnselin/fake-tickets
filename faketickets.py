##########################################
#   
#   Romain Anselin - DataStax - 2022
#   File: faketickets.py
#   Function: manage specific faketickets
#       related queries 
#
##########################################

import random
from datetime import datetime, timedelta
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import PreparedStatement
import helper

### Fake_tickets
def create_table_fake_tickets(session, ksname, tblname):
    session.execute("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + """(
        id int, 
        ownedby text, 
        ticket int, 
        time timestamp, 
        updatedat timestamp, 
        notes text,
        PRIMARY KEY (id)
        );
        """)

### Sample inserts
def insert_bind_fake_tickets(session, ksname, tblname):
    ticket_insert = "INSERT INTO " + ksname + "." + tblname + "(id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?) ;"
    ticketins_prep = session.prepare(ticket_insert)
    ticketins_prep.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return ticketins_prep

def insert_bind_fake_tickets_ttl(session, ksname, tblname):
    ttl = 86400
    ticket_insert = "INSERT INTO " + ksname + "." + tblname + "(id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?) USING TTL " + ttl + " ;"
    ticketins_prep = session.prepare(ticket_insert)
    ticketins_prep.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return ticketins_prep

def insert_bind_fake_tickets_lwt(session, ksname, tblname):
    ticket_insert = "INSERT INTO " + ksname + "." + tblname + "(id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS ;"
    ticketins_prep = session.prepare(ticket_insert)
    ticketins_prep.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    ticketins_prep.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
    return ticketins_prep

### Materizalized view
def create_mv_fake_tickets(session, ksname, tblname):
    mvname='mv_fake_tickets'
    session.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS " + ksname + "." + mvname + """(
        AS select ownedby, time, id, notes, ticket 
        FROM """ + ksname + "." + tblname + """
        WHERE  ownedby is not null PRIMARY KEY (ownedby, id) ;
        """)

### SELECT and COUNT
def select_bind_fake_tickets(session, ksname, tblname):
    ticket_select = """
        SELECT id,  ownedby, ticket, time, notes
        FROM """ + ksname + "." + tblname + """
        WHERE id = ?
        """
    ticketsel_prep = session.prepare(ticket_select)
    ticketsel_prep.consistency_level=ConsistencyLevel.QUORUM
    return ticketsel_prep

def select_count_bind_fake_tickets(session, ksname, tblname):
    ticket_sel_count = """
        SELECT count(*)
        FROM """ + ksname + "." + tblname + """
        WHERE id = ?
        """
    ticketselcnt_prep = session.prepare(ticket_sel_count)
    return ticketselcnt_prep

# Create SAI
def create_sai_fake_tickets(session, ksname, tblname):
    idx = "ownedby"
    # !!! TIL: Originally, index called dept_sai_idx hardcoded. It will not create the index as it exists for another table, but it will not error out either !!!
    try:
        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS " + tblname + "_sai_idx ON " + ksname + "." + tblname + "(" + idx + ") USING 'StorageAttachedIndex'")
    except:
        print ("ERROR: Unable to create index:\n" "CREATE CUSTOM INDEX IF NOT EXISTS " + tblname + "_sai_idx ON " + ksname + "." + tblname + "(" + idx + ") USING 'StorageAttachedIndex'")

def create_solr_fake_tickets(session, ksname, tblname):
    print("Creating Solr Index on %s.%s" % (ksname, tblname))
    session.execute("CREATE SEARCH INDEX IF NOT EXISTS ON " + ksname + "." + tblname + " WITH COLUMNS ownedby, updatedat, time, notes")

def fake_tickets_workflow(session, ksname, startval, numrec):
    ### Values implementation
    tblname = "fake_tickets"
    endval = startval + numrec
    owner = [ "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan", "Alberto", "Peter", "Pav", "Alkesh", "Uzoma", "Calum", "Cordell" ]
    power10 = [1,10,100,1000,10000,100000,1000000,10000000]

    # Create table if not exists
    create_table_fake_tickets(session, ksname, tblname)
    create_sai_fake_tickets(session, ksname, tblname)
    # Insert Bind
    ticketins_prep = insert_bind_fake_tickets(session, ksname, tblname)

    # Select and count binds
    ticketsel_prep = select_bind_fake_tickets(session, ksname, tblname)
    ticketselcnt_prep = select_count_bind_fake_tickets(session, ksname, tblname)

    # min_time = datetime.now()
    min_time = helper.current_milli_time()

    for i in range (startval, endval):
        # timenow = datetime.now()
        timenow = helper.current_milli_time()
        pickowner = str(random.choice(owner))
        pickticket = random.randint(0,100000)
        if i%2 == 0:
            datastr = 'Python writing data with value ' + str(i) + ' for ' + pickowner + ' at ' + str(helper.fromts(timenow))
        else:
            datastr = '%s Odd entries have a different data string for record %s for %s' % (str(helper.fromts(timenow)), str(i), pickowner)

        ### Bound statement
        tickins_bind = ticketins_prep.bind((i, pickowner, pickticket, timenow, datastr))
        # print(str(tickins_bind))
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

            ### COUNT SCENARIO
            tickselcnt_bind = ticketselcnt_prep.bind([i])
            cntrows = session.execute(tickselcnt_bind)
            for row in selrows:
                print("Number of rows: " + str(row.count))

            print( "Read execution finished: " + str( helper.dtn() - startselread ) )

            # saiSimplestmt = SimpleStatement( "SELECT id,  ownedby, ticket, time, notes FROM " + ksname + "." + tblname + " WHERE time >= %s AND time <= %s;" % (min_time, timenow) ,
            #    consistency_level=ConsistencyLevel.QUORUM)

            # startsairead = helper.dtn()

            # sai_rows = session.execute(saiSimplestmt)
            
            # for sai_row in sai_rows:
            #     print(sai_row.id, sai_row.ownedby, sai_row.ticket, sai_row.time, sai_row.notes)
            # print( "SAI READ " + str(i) + " FINISHED " + str( helper.dtn() - startsairead ) )

            # saicntSimplestmt = SimpleStatement( "select count(*) FROM " + ksname + "." + tblname + " WHERE my_id = %s AND time >= %s AND time <= %s;" % (i, min_time, timenow) ,
            #     consistency_level=ConsistencyLevel.QUORUM)
            # saicnt_rows = session.execute(saicntSimplestmt)
            # for saicnt_row in saicnt_rows:
            #   print("Number of SAI rows: " + str(sai_row.count))
            # saicnt_rows = session.execute(saicntSimplestmt)
