# Romain Anselin - 2022/12
import os
import sys
import re
import helper
import argparse
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassqueries as cq
import faketickets as ft
#import deletebackups as dlb
#import logate
import astrasets

### Change file here!
#conf_file = 'conf/conf_shared68.ini'

# Check arguments
# (note 2 includes arg 0 which is this script!)
# if len(sys.argv) == 1:
#     # Default values if none set
#     startval=0
#     numrec=1000000
# elif len(sys.argv) == 3:
#     print("debug arg0 ",sys.argv[0]," arg1 ",sys.argv[1]," arg2 ",sys.argv[2],"\n")
#     startval=int(sys.argv[1])
#     numrec=int(sys.argv[2])
# elif len(sys.argv) != 3:
#     print(
#             "\n***",sys.argv[0], "***\n"
#             'Incorrect number of arguments, please run script as follows:'
#             '\n\n'+str(sys.argv[0])+' <nothing if you want to use default values>'
#             '\n\n'+str(sys.argv[0])+' <start value> <number of records to insert>'
#         )
#     sys.exit(0)

def readrecords(incf):
    if os.path.exists(incf):
        with open(incf, 'r') as file:
            # for line in file:
                # Assuming the values are separated by a tab ('\t')
            startval, numrec = [int(x) for x in next(file).split()] # read first line           
            #print(f'Start: {startval}')
            #print(f'Step : {numrec}')
        return startval,numrec
        file.close()
    else:
        print(f'Error: The specified input file "{incf}" does not exist.')
        exit

def writerecords(incf, startval, numrec):
    if os.path.exists(incf):
        newval = startval + numrec
        file = open(incf, 'w')
        file.write("%i %i" % (newval, numrec))
        file.close()
    

def arguments():
    parser = argparse.ArgumentParser(description='Fake tickets script to inject and read data on Cassandra/DSE')
    parser.add_argument('-c', '--conf', type=str, required=True, help="Configuration file to connect")
    parser.add_argument('-t', '--table', type=str, required=True, help='Define table name to insert data to')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-v', '--values', nargs=2, type=int, default=[0,1000000], help="Values to start from and number of records to insert")
    #parser.add_argument('-n', '--numrec', type=int, default=1000000, help="Number of records to insert")
    group.add_argument('-i', '--incfile', type=str, help='File to leverage to define values to use')
    args = parser.parse_args()

    conf_file = args.conf
    table = args.table
    startval, numrec = args.values
    incf = args.incfile

    if incf is not None:
        startval, numrec = readrecords(incf)
        writerecords(incf, startval, numrec)

    return conf_file, startval, numrec, incf, table

conf_file, startval, numrec, incf, tblname = arguments()


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
        # cluster = Cluster([host1,host2])
        cluster = Cluster([host1])

session = cluster.connect()

# Get release
cq.simple_test_query(session)

# Create Keyspace if necessary
if isAstra == "false":
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + ksname + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + dcname + "': '3'} ; ")

### Starting routine
#dlb.opscenter_delete(session)
ft.fake_tickets_workflow(session, ksname, startval, numrec, tblname)
#astrasets.create_table_sets(session)
#astrasets.random_set_insert(session)
#astrasets.read_data(session)

# logate.logate_workflow(session, ksname, startval, numrec, tblname)
### Create multiple tables + Solr encrypted core !!! Use with Caution as per DSP-
# cq.solr_encrypted_tables_create(session)

### Create ks/table
# cq.create_table_onepk_gc600(session, ksname, "onepk_gc600")

### Truncate table
# cq.truncate_table(session, ksname, tblname)
