##########################################
#
#   Romain Anselin - DataStax - 2022
#   File: faketickets.py
#   Function: manage specific faketickets
#       related queries
#
##########################################

import logging
import random
import time
from datetime import datetime, timedelta
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import PreparedStatement
from cassandra.concurrent import execute_concurrent_with_args
import helper
#import randomVal

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

def insert_bind_fake_tickets_ttl(session, ksname, tblname, ttl):
    #ttl = 86400
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

# ---------------------------------------------------------------------------
# Proposal 1: Paced Asynchronous Ingestion
# ---------------------------------------------------------------------------

def insert_paced_async(session, prepared_stmt, arg_tuples, concurrency=50, target_ops=200):
    """
    Flush a list of bound-argument tuples using execute_concurrent_with_args
    with controlled concurrency and smooth rate pacing.

    Args:
        session:        Active Cassandra session.
        prepared_stmt:  PreparedStatement to bind each tuple against.
        arg_tuples:     Iterable of argument tuples matching the prepared statement.
        concurrency:    Maximum number of in-flight requests at once (default 50).
        target_ops:     Target operations/second overall (default 200). Set to 0 to disable pacing.
    """
    chunk_size = concurrency * 2
    sleep_delay = chunk_size / target_ops if target_ops > 0 else 0
    arg_list = list(arg_tuples)

    for offset in range(0, len(arg_list), chunk_size):
        chunk = arg_list[offset:offset + chunk_size]
        results = execute_concurrent_with_args(session, prepared_stmt, chunk, concurrency=concurrency)
        for success, result in results:
            if not success:
                print(f"Write failure: {result}")
        if sleep_delay > 0:
            time.sleep(sleep_delay)


def fake_tickets_workflow(session, ksname, startval, numrec, tblname, concurrency=50, target_ops=200):
    ### Values implementation
    #tblname = "fake_tickets_ttl"
    endval = startval + numrec
    owner = [ "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan", "Alberto", "Peter", "Pav", "Cordell", "Frez", "Adjete", "Paco", "Pranjal", "Yuva", "Anjali", "Suraj" ]
    power10 = [1,10,100,1000,10000,100000,1000000,10000000]
    #ttl = randomVal()

    # Create table if not exists
    create_table_fake_tickets(session, ksname, tblname)
    #create_sai_fake_tickets(session, ksname, tblname)
    # Insert Bind
    ticketins_prep = insert_bind_fake_tickets(session, ksname, tblname)

    # Select and count binds
    ticketsel_prep = select_bind_fake_tickets(session, ksname, tblname)
    ticketselcnt_prep = select_count_bind_fake_tickets(session, ksname, tblname)

    chunk_size = concurrency * 2
    pending_args = []

    for i in range(startval, endval):
        # timenow = datetime.now()
        timenow = helper.current_milli_time()
        pickowner = str(random.choice(owner))
        pickticket = random.randint(0,100000)

        if i%2 == 0:
            datastr = 'Python writing data with value ' + str(i) + ' for ' \
                + pickowner + ' at ' + str(helper.fromts(timenow))
        else:
            datastr = '%s Odd entries have a different data string for record %s for %s' \
                 % (str(helper.fromts(timenow)), str(i), pickowner)

        pending_args.append((i, pickowner, pickticket, timenow, datastr))

        # Flush a full chunk asynchronously
        if len(pending_args) >= chunk_size:
            insert_paced_async(session, ticketins_prep, pending_args, concurrency=concurrency, target_ops=target_ops)
            pending_args = []

        # Show time for every 10k records
        if i%10000 == 0:
            print( "Written " + str(i) + " records so far, time now " + str(helper.dtn()) )

        # Read every power of 10 the content
        if i in power10:
            # Flush any buffered writes before reading to ensure the row is visible
            if pending_args:
                insert_paced_async(session, ticketins_prep, pending_args, concurrency=concurrency, target_ops=target_ops)
                pending_args = []

            print( "Record " + str(i) + " reading now " + str( helper.dtn() ) )
            startselread = helper.dtn()
            ### READ SCENARIO
            ticksel_bind = ticketsel_prep.bind([i])
            session.execute(ticksel_bind)

            ### COUNT SCENARIO
            tickselcnt_bind = ticketselcnt_prep.bind([i])
            cntrows = session.execute(tickselcnt_bind)
            cnt_val = cntrows.one()[0] if cntrows else 0
            print("Number of rows: " + str(cnt_val))

            print( "Read execution finished: " + str( helper.dtn() - startselread ) )

    # Flush any remaining buffered writes
    if pending_args:
        insert_paced_async(session, ticketins_prep, pending_args, concurrency=concurrency, target_ops=target_ops)


### TWCS + TTL functions

def create_table_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=86400, window_size=1, window_unit='HOURS'):
    """
    Creates a table configured with Time Window Compaction Strategy (TWCS)
    and a default TTL (2 days = 172800 seconds).
    """
    cql = f"""
    CREATE TABLE IF NOT EXISTS {ksname}.{tblname} (
        id int,
        ownedby text,
        ticket int,
        time timestamp,
        updatedat timestamp,
        notes text,
        PRIMARY KEY (id)
    )
    WITH default_time_to_live = {ttl_seconds}
    AND compaction = {{
        'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',
        'compaction_window_size': '{window_size}',
        'compaction_window_unit': '{window_unit}',
        'timestamp_resolution': 'MILLISECONDS',
        'unchecked_tombstone_compaction': 'true'
    }}
    AND gc_grace_seconds = 10800;
    """
    session.execute(cql)

def insert_bind_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=None):
    """
    Prepares an INSERT statement. If ttl_seconds is provided, uses 'USING TTL <value>'.
    Otherwise, relies on the table-level default_time_to_live.
    """
    if ttl_seconds is not None:
        query = f"INSERT INTO {ksname}.{tblname} (id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?) USING TTL {ttl_seconds};"
    else:
        query = f"INSERT INTO {ksname}.{tblname} (id, ownedby, ticket, time, notes) VALUES (?, ?, ?, ?, ?);"
    prepared = session.prepare(query)
    prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return prepared

def fake_tickets_twcs_workflow(
    session, ksname, startval, numrec, tblname,
    ttl_seconds=86400, concurrency=50, target_ops=1000,
    overlap_interval_min=30, overlap_lookback_min=120
):
    """
    Executes continuous ingestion into a TWCS-compacted table using paced asynchronous writes.
    Rate pacing is now controlled via concurrency and target_ops (see insert_paced_async).

    Overlap injection:
        Every `overlap_interval_min` minutes worth of ingestion (measured in IDs at
        target_ops rows/s), one extra write is injected for a randomly chosen ID from
        the past `overlap_lookback_min` minutes of this process's own history.
        This reproduces cross-window partition rewrites for TWCS compaction analysis.
        All overlap events are written to overlap_<tblname>.log with a full timestamp.

    Args:
        overlap_interval_min:  Minutes of equivalent ingestion between overlap events (default 30).
        overlap_lookback_min:  Minutes of equivalent ingestion that define the candidate
                               pool for the overlap rewrite (default 120 = 2 hours).
    """
    # 1. Provision Table
    create_table_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=ttl_seconds, window_size=1, window_unit='HOURS')

    # 2. Prepare Insert (relies on table default TTL)
    prepared_insert = insert_bind_fake_tickets_twcs(session, ksname, tblname, ttl_seconds=None)

    # 3. Overlap logger — file only, does not touch the root/stdout logger
    overlap_log_path = f"overlap_{tblname}.log"
    overlap_logger = logging.getLogger(f"overlap.{tblname}")
    overlap_logger.setLevel(logging.INFO)
    if not overlap_logger.handlers:
        fh = logging.FileHandler(overlap_log_path)
        fh.setFormatter(logging.Formatter("%(asctime)s %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        overlap_logger.addHandler(fh)
    overlap_logger.propagate = False

    # 4. Derive overlap thresholds entirely from target_ops — no hardcoded values.
    #    At target_ops rows/s, N minutes of ingestion corresponds to
    #    target_ops * N * 60 IDs.
    overlap_every    = target_ops * overlap_interval_min * 60   # IDs between overlap events
    overlap_lookback = target_ops * overlap_lookback_min * 60   # candidate pool width

    owner_pool = [
        "Romain", "Ryan", "Bettina", "Navaneetha", "Rachan", "Ivan",
        "Alberto", "Peter", "Pav", "Cordell", "Frez", "Adjete",
        "Paco", "Pranjal", "Yuva", "Anjali", "Suraj"
    ]

    chunk_size = concurrency * 2
    pending_args = []
    i = startval

    while True:
        timenow = helper.current_milli_time()
        pickowner = random.choice(owner_pool)
        pickticket = random.randint(0, 100000)

        if i % 2 == 0:
            datastr = f"TWCS data record {i} for {pickowner} at {helper.fromts(timenow)}"
        else:
            datastr = f"{helper.fromts(timenow)} TWCS odd entry payload for record {i} owned by {pickowner}"

        pending_args.append((i, pickowner, pickticket, timenow, datastr))

        # --- Overlap injection ---
        # Fires every overlap_every IDs (≈ overlap_interval_min minutes at target_ops).
        # Only once enough history exists: at least overlap_lookback IDs written.
        # Pool: [max(startval, i - overlap_lookback), i - 1] — own history only.
        if (i - startval) > 0 and (i - startval) % overlap_every == 0:
            pool_lo = max(startval, i - overlap_lookback)
            pool_hi = i - 1
            overlap_id = random.randint(pool_lo, pool_hi)
            o_owner  = random.choice(owner_pool)
            o_ticket = random.randint(0, 100000)
            o_datastr = (
                f"*** OVERLAP REWRITE | original_id={overlap_id} "
                f"| rewrite_at_counter={i} | owner={o_owner} "
                f"| written_at={helper.fromts(timenow)} ***"
            )
            pending_args.append((overlap_id, o_owner, o_ticket, timenow, o_datastr))
            overlap_logger.info(
                f"OVERLAP id={overlap_id} pool=[{pool_lo},{pool_hi}] "
                f"trigger_i={i} owner={o_owner} ticket={o_ticket}"
            )

        i += 1

        # Flush a full chunk asynchronously
        if len(pending_args) >= chunk_size:
            insert_paced_async(session, prepared_insert, pending_args, concurrency=concurrency, target_ops=target_ops)
            pending_args = []

        if i % 10000 == 0:
            print(f"[{helper.dtn()}] TWCS Ingestion: {i} records inserted.")
