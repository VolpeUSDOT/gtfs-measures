#-------------------------------------------------------------
#
# Utility functions for printing progress and start/end timing.
#
# Stephen Zitzow-Childs
#
# Volpe National Transportation Systems Center, USDOT
#
# Last modified: 6/18/2017
# Note: Newer version of this needs to be loaded and incorporated (7/20/2017)
#-------------------------------------------------------------

import sys
import datetime

def update():
     sys.stdout.write('.')
     sys.stdout.flush()
     return

def final():
     sys.stdout.write('\n')
     sys.stdout.flush()
     return
 
def print_start_time():
    start_time = datetime.datetime.now()
    print('')
    print('Start at {:%Y-%m-%d %H:%M:%S}'.format(start_time))
    print('=====================================================')
    return(start_time)

def print_end_time(start_time):
    end_time = datetime.datetime.now()
    total_run_time = (end_time - start_time)

    # Calculate duration
    hours = total_run_time.seconds / 3600
    mins  = (total_run_time.seconds % 3600) / 60
    secs  = (total_run_time.seconds % 3600) % 60
    
    print('End at {:%Y-%m-%d %H:%M:%S}  --  Elapsed {:02d}:{:02d}:{:02d}'.format(end_time, hours, mins, secs))
    print('=====================================================')
    print('')