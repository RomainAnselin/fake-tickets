from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import PreparedStatement

'''
# Table format
CREATE TABLE "OpsCenter".backup_reports (
    week text,
    event_time timestamp,
    backup_id text,
    type text,
    destination text,
    deleted_at timestamp,
    full_status text,
    keyspaces text,
    status text,
    PRIMARY KEY (week, event_time, backup_id, type, destination)

# Sample data
fred@cqlsh> select week, event_time, backup_id, type, status from "OpsCenter".backup_reports where week='202250' ;

 week   | event_time                      | backup_id                                                              | type   | status
--------+---------------------------------+------------------------------------------------------------------------+--------+---------
 202250 | 2022-12-13 13:00:00.000000+0000 | opscenter_51b4a197-c860-47a3-815b-d200854f0631_2022-12-13-13-00-00-UTC | backup | success
 202250 | 2022-12-13 13:00:00.000000+0000 | opscenter_51b4a197-c860-47a3-815b-d200854f0631_2022-12-13-13-00-00-UTC | backup | success
 202250 | 2022-12-12 17:14:45.000000+0000 |                                opscenter_adhoc_2022-12-12-16-39-19-UTC | delete | success
 202250 | 2022-12-12 17:12:26.000000+0000 |                                opscenter_adhoc_2022-12-12-13-00-30-UTC | delete | success
'''

# Delete anything related to adhoc backups in OpsCenter for given week (make sure to clear said snapshots first as this just remove UI entries)
def opscenter_delete(session):
    ksname = '"OpsCenter"'
    tblname = 'backup_reports'
    wk = '202250'

    opscselect = """
        SELECT *
        FROM """ + ksname + "." + tblname + """
        WHERE week = ?
        """
    opscsel_prep = session.prepare(opscselect)
    opscsel_prep.consistency_level=ConsistencyLevel.QUORUM

    opsc_bind = opscsel_prep.bind([wk])
    opscbackup = session.execute(opsc_bind)

    for row in opscbackup:
        if "adhoc" in row.backup_id:
            print("DELETE FROM " + ksname + "." + tblname,
                " WHERE week='202250' AND event_time = '" + str(row.event_time) + "'",
                " AND backup_id = '" + str(row.backup_id) + "'",
                " AND type = '" + row.type + "'",
                " AND destination = '" + row.destination + "';")