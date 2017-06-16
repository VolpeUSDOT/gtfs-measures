#------------------------------------------------------------------------------
# Name:        GTFS Ingest
# Purpose:     To ingest a GTFS feed into a sqlite file
#
# Author:      Alex Oberg
# Edited:      Stephen Zitzow-Childs
#
# Created:     9/12/2016
# Updated:     4/27/2017
#
# Script requries the pygtfs Python module.
#
# How to install pygtfs:
# Run the following in command line (Assume pip is already installed, you may 
# need to modify Python path)
# C:\Python27\ArcGIS10.3\python.exe -m pip install -U pygtfs
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#
#------------------------------------------------------------------------------

import datetime
import pygtfs
import os

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))

# Select input feed folder and output database
gtfs_feed= r"C:\GTFS\ValleyMetro"
output_sqlite = r"C:\GTFS\GTFS-Test.sqlite"

#Delete sqlite output if it already exists
try:
    os.remove(output_sqlite)
    print "Deleted existing sqlite output"
except OSError:
    pass

#Create blank sqlite file
sched = pygtfs.Schedule(output_sqlite)

print(sched)
#Ingest GTFS feed into sqlite file
pygtfs.append_feed(sched, gtfs_feed)

print ("Done creating sqlite file")

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))