import random
from datetime import datetime
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
def solr_encrypted_tables_create(session, ksname):
    tblname='encrypted_index'
    for i in range (1,54):
        print ("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + str(i) + "(id int primary key, ownedby text, ticket int, time text)")
        session.execute("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + str(i) +"(id int primary key, ownedby text, ticket int, time text)")
        session.execute("CREATE SEARCH INDEX IF NOT EXISTS ON " + ksname + "." + tblname + str(i) +" WITH COLUMNS ownedby, ticket AND CONFIG { directoryFactory:'encrypted' };")

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

def create_table_dualid(session, ksname, tblname):
    session.execute("CREATE TABLE IF NOT EXISTS " + ksname + "." + tblname + """(
        id int,
        id2 int,
        ownedby text, 
        ticket int, 
        time timestamp, 
        updatedat timestamp, 
        notes text,
        PRIMARY KEY (id, id2)
        );
        """)
    
def insert_bind_dualid(session, ksname, tblname):
    dualid_insert = "INSERT INTO " + ksname + "." + tblname + "(id, id2, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?, ?) USING TTL 86400 ;"
    dualid_prep = session.prepare(dualid_insert)
    dualid_prep.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return dualid_prep

### SELECT and COUNT
def select_bind_dualid(session, ksname, tblname):
    dualid_select = """
        SELECT id, id2, ownedby, ticket, time, notes
        FROM """ + ksname + "." + tblname + """
        WHERE id IN ?
        """
    dualidsel_prep = session.prepare(dualid_select)
    dualidsel_prep.consistency_level=ConsistencyLevel.QUORUM
    return dualidsel_prep

def dualid_workflow(session, ksname, startval, numrec):
    ### Values implementation
    tblname="dualid"
    endval=startval+numrec
    owner = [ "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan", "Alberto", "Peter", "Pav", "Alkesh", "Uzoma", "Calum", "Cordell" ]
    power10 = [1,10,100,1000,10000,100000,1000000,10000000]

    # Create table if not exists
    create_table_dualid(session, ksname, tblname)

    # Insert Bind
    dualidins_prep = insert_bind_dualid(session, ksname, tblname)

    # Select and count binds
    dualidsel_prep = select_bind_dualid(session, ksname, tblname)
    #ticketselcnt_prep = select_count_bind_fake_tickets(session, ksname, tblname)

    # min_time = datetime.now()
    min_time = helper.current_milli_time()

    for i in range (0,100):
        if i == 0:
            myin = str(i)
        else:
            myin = str(myin) + ', ' + str(i)

        # for y in range (0,1000):
        #     # timenow = datetime.now()
        #     timenow = helper.current_milli_time()
        #     pickowner = str(random.choice(owner))
        #     pickticket = random.randint(0,100000)
        #     if i%2 == 0:
        #         datastr = 'Python writing data with value ' + str(i) + ' for ' + pickowner + ' at ' + str(helper.fromts(timenow))
        #     else:
        #         datastr = '%s Odd entries have a different data string for record %s for %s' % (str(helper.fromts(timenow)), str(i), pickowner)

        #     ### Bound statement
        #     dualidins_bind = dualidins_prep.bind((i, y, pickowner, pickticket, timenow, datastr))
        #     session.execute(dualidins_bind)

        if i == 99:
            before = datetime.now()
            print(str(datetime.now()))
            stmt = SimpleStatement("select id, id2, notes, time FROM " + ksname + "." + tblname + " WHERE id IN (%s);" % (myin),
                consistency_level=ConsistencyLevel.QUORUM)
            print(stmt)
            rows = session.execute(stmt)
            after = datetime.now()
            for row in rows:
                cnt =+ 1 
            print(str(after - before), cnt)
