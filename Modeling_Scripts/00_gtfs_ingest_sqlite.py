#------------------------------------------------------------------------------
# Name:        GTFS Ingest
# Purpose:     To ingest a GTFS feed into a sqlite file
#
# Author:      Alex Oberg
# Edited:      Stephen Zitzow-Childs
#
# Created:     9/12/2016
# Updated:     6/18/2017
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

import pygtfs
import os
import my_utils

start_time = my_utils.print_start_time()

GTFS_folder = 'agency/'

# Select input feed folder and output database
dir_path = os.getcwd()
gtfs_feed= os.path.join(dir_path,GTFS_folder)
output_sqlite = 'gtfs.sqlite'

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

print("Done creating sqlite file")

my_utils.print_end_time(start_time)