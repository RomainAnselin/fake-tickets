##########################################
#   
#   Romain Anselin - DataStax - 2022
#   File: helper.py
#   Function: complementary functions to
#       astra-insert that dont fit in the 
#       main files
#
##########################################

import multiprocessing
import configparser
from datetime import datetime
import time
import sys
import math
from os.path import exists

# Read astra config
def read_config(conf_file):
    if exists(conf_file):
        print("Using conf file: " + conf_file)
        config = configparser.ConfigParser()
        config.read(conf_file)
        return config
    else:
        sys.exit("ERROR: File not found %s, \n Exiting now... ", conf_file)

### Unix Epoch ms
def current_milli_time():
    timenow = round(time.time() * 1000)
    # print ( "timenow: " + str(timenow) )
    # mstime = datetime.utcnow().isoformat(sep=' ', timespec='milliseconds')
    return timenow

def fromts(myts):
    # TOFIX: This is BUGGY and need to be changed. When ms is 0xx, it trims the first 0 and gives an odd value
    # ie: 2022-04-23 11:12:50.079000 becomes 2022-04-23 11:12:50:79
    tsnow = time.strftime('%Y-%m-%d %H:%M:%S:{}'.format(myts%1000), time.gmtime(myts/1000.0))
    #tsnow = datetime.fromtimestamp(myts/1000)
    #print ( "myts: " + str(myts) + " tsnow: " + str(tsnow) )
    #s = tsnow.isoformat(timespec='milliseconds')
    #print(s)
    return tsnow

### If want to test a value as a power of 10
### Too resource intensive. Better using a list (10/100/1000...)
### DEPRECATED
def isPower10 (num, base):
    if num == 0: 
        return False
    elif base in {0, 1}:
        return num == base
    power = int (math.log (num, base) + 0.5)
    return base ** power == num

def dtn():
    tsp = datetime.now()
    return tsp

def cpurange(startval,numrec):
    # How many CPU
    cpucnt = multiprocessing.cpu_count() - 1
    # how many size of ranges
    valpercpu = math.floor(numrec/cpucnt)
    rest = numrec - valpercpu*cpucnt
    return cpucnt, valpercpu, rest


# startval = 0
# numrec = 11000
# cpucnt, valpercpu, rest = cpurange(startval,numrec)
# for cpunum in range(0,cpucnt):
#     if cpunum == 0:
#         step = startval+valpercpu+rest
#     else:
#         step = valpercpu
#     print ("cpu" + str(cpunum) + " range: " + str(startval) + " end: " + str(step))
#     startval += step


def record(increment):
    # Specify the file path
    file_path = "record.txt"

    # Try to open the file for reading
    try:
        with open(file_path, 'r') as file:
            # Read the existing value from the file
            existing_value = int(file.read())
    except FileNotFoundError:
        # If the file doesn't exist, set the default value to 0
        existing_value = 0
    except ValueError:
        # Handle the case where the file contains non-integer data
        print("Error: File contains non-integer data.")
        existing_value = 0

    # Do some operations with the existing value, for example, increment it
    new_value = existing_value + increment

    # Write the new value back to the file
    with open(file_path, 'w') as file:
        file.write(str(new_value))

    # Print the new value for verification
    print("New value:", new_value)
    return 