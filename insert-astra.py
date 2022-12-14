# Romain Anselin - 2022/12
import os
import sys
import re
import helper
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassqueries as cq
import faketickets as ft
import deletebackups as dlb

### Change file here!
conf_file = '/home/romain/dev/fake-tickets/conf_bootcamp.ini'

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
config = helper.read_config(conf_file)
isAstra = config['general']['isAstra']
dcname = config['general']['dcname']
ksname = config['general']['ksname']
#tblname = config['general']['tblname']

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
        cluster = Cluster([host1], auth_provider=auth_provider)
    else:
        cluster = Cluster([host1,host2])

session = cluster.connect()

# Get release
cq.simple_test_query(session)

# Create Keyspace if necessary
if isAstra == "false":
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksname + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + dcname + "': '1'} ; ")

### Starting routine
#dlb.opscenter_delete(session)
ft.fake_tickets_workflow(session, ksname, startval, numrec)

### Create multiple tables + Solr encrypted core !!! Use with Caution as per DSP-
# cq.solr_encrypted_tables_create(session)

### Create ks/table
# cq.create_table_onepk_gc600(session, ksname, "onepk_gc600")

### Truncate table
# cq.truncate_table(session, ksname, tblname)