#-------------------------------------------------------------------------------
# Name:        GTFS Ingest
# Purpose: To ingest a GTFS feed into a sqlite file
#
# Author:      Alex Oberg
#
# Created:     9/12/2016
# Updated:     6/15/2017

# Script requries the pygtfs Python module.

# How to install pygtfs:
# Run the following in command line (Assume pip is already installed, you may need to modify Python path)
# C:\Python27\ArcGIS10.3\python.exe -m pip install -U pygtfs


#-------------------------------------------------------------------------------

import datetime
import pygtfs
import os

start_time = datetime.datetime.now()
print('\nStart at ' + str(start_time))

# CONFIG
# Update the following paths as needed

#BART
gtfs_feed= r"C:\tasks\2016_09_12_GTFS_ingest\Model\BART\gtfs_feed_from_stephen"
output_sqlite = r"C:\tasks\2016_09_12_GTFS_ingest\Model\BART\GTFS-BART.sqlite"
#Gary_MBTA
#gtfs_feed= r"D:\projects\gtfs_perf_meas\Input\MBTA_GTFS_71_73"
#output_sqlite = r"D:\projects\gtfs_perf_meas\Input\mbta.sqlite"

# MAIN

#Delete sqlite output if it already exists
try:
    os.remove(output_sqlite)
    print "Deleted existing sqlite output"
except OSError:
    pass

#Create blank sqlite file
sched = pygtfs.Schedule(output_sqlite)

#Ingest GTFS feed into sqlite file
pygtfs.append_feed(sched, gtfs_feed)

print ("Done creating sqlite file")

end_time = datetime.datetime.now()
total_time = end_time - start_time
print ("\nEnd at {}.  Total run time {}".format(end_time, total_time))
