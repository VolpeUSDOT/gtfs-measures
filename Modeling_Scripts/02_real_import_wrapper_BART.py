
import sqlite3
import datetime
import csv
import xlrd
import shortest_path

def findMatch(shortname, namelist):
     matchval = []
     for row in namelist:
          if row[1] == shortname:
               matchval = row[0]
               break
     return(matchval)


start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='

#----------
# CONSTANTS FOR ANALYSIS
REAL_FILE = 'C:/GTFS/BART/BART Ridership_September2016.xlsx'
conn = sqlite3.connect('GTFS-BART.sqlite')
db_cursor = conn.cursor()

#----------
# Step 1
print '======================================================================='
print 'Step 1: Load BART descriptive data'
print str(datetime.datetime.now())

nodelist = []
with open('C:/GTFS/BART/BARTstops.csv','rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     for row in infile:
          nodelist.append(unicode(row[0]))
          
namelist = []
with open('C:/GTFS/BART/BARTstopnamemap.csv','rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     for row in infile:
          namelist.append([unicode(row[0]),unicode(row[1])])

segmentlist = []
maxdist = 0
with open('C:/GTFS/BART/BARTonewaylinks.csv','rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     for row in infile:
          segmentlist.append([unicode(row[0]),unicode(row[1]),int(row[2])])
          maxdist += int(row[2])
maxdist += 1

# END Step 1
#----------


#----------
# Step 2
print '======================================================================='
print 'Step 2: Load ridership data and find paths between nodes'
print str(datetime.datetime.now())

# Load the data from the Car Following Instances tab
wb = xlrd.open_workbook(REAL_FILE)
weekday_sheet = wb.sheet_by_name('Weekday OD')

# Load the entrances from the 2nd row (also take care of the numeric stops)
entrances = []
for c in range(1,weekday_sheet.ncols-2):
     newval = weekday_sheet.cell(1,c).value
     if isinstance(newval,float):
          newval = unicode(int(newval))
     entrances.append(newval)
     
# Load each exit and assign ridership to a large table
ridershipdata = []
for r in range(2,weekday_sheet.nrows-2):
     # This exit point
     this_exit = weekday_sheet.cell(r,0).value
     if isinstance(this_exit,float):
          this_exit = unicode(int(this_exit))
     for c in range(len(entrances)):
          this_entrance = entrances[c]
          if this_entrance != this_exit:
               this_rider = weekday_sheet.cell(r,c+1).value
               [path, dist, tracker] = shortest_path.findpath(nodelist, segmentlist, this_entrance, this_exit, maxdist, False)
          
               for i in range(len(path)-1):
                    from_node = path[i]
                    to_node = path[i+1]
                    ridershipdata.append([from_node, to_node, this_rider, this_entrance, this_exit])

          
# END Step 2
#----------


#----------
# Step 3
print '======================================================================='
print 'Step 3: Push to the database in this raw form, then group'
print str(datetime.datetime.now())

db_cursor.execute('drop table if exists real_redundant')
db_cursor.execute('create table real_redundant (fromnode text, tonode text, riders float, masterorig text, masterdest text)')
db_cursor.executemany('insert into real_redundant values (?,?,?,?,?)',ridershipdata)
conn.commit()

db_cursor.execute('drop table if exists real_aggregated')
db_cursor.execute('create table real_aggregated as select fromnode, tonode, sum(riders) as tot_ridership from real_redundant group by fromnode, tonode')
conn.commit()

# END Step 3
#----------


#----------
# Step 4
print '======================================================================='
print 'Step 4: Correct the 2-letter name real data to 4-letter name stops and set up for conflation'
print str(datetime.datetime.now())

print '... getting aggregated data to fix'
db_cursor.execute('select fromnode, tonode, tot_ridership from real_aggregated')
unfixed = db_cursor.fetchall()

print '... fix names'
fixed_final = []
for row in unfixed:
     this_from = row[0]
     this_to = row[1]
     this_ridership = row[2]
     
     # Special cases
     if this_from == unicode('12'):
          # 12th has no options for alternate naming
          new_from = unicode('12TH')
          if this_to == unicode('19'):
               # This must be northbound
               new_to = unicode('19TH_N')
          else:
               new_to = findMatch(this_to,namelist)
               
     elif this_from == unicode('19'):
          # Only two alternatives
          if this_to == unicode('12'):
               # Special pairing
               new_from = unicode('19TH')
               new_to = unicode('12TH')
          elif this_to == unicode('MA'):
               # Special pairing
               new_from = unicode('19TH_N')
               new_to = unicode('MCAR')
               
     elif this_from == unicode('AS'):
          # Ashbury has no options for alternate naming
          new_from = unicode('ASHB')
          if this_to == unicode('MA'):
               # This must be southbound
               new_to = unicode('MCAR_S')
          else:
               new_to = findMatch(this_to,namelist)
               
     elif this_from == unicode('MA'):
          # Only three alternatives
          if this_to == unicode('19'):
               # This must be southbound
               new_from = unicode('MCAR_S')
               new_to = unicode('19TH')
          elif this_to == unicode('AS'):
               # This must be northbound
               new_from = unicode('MCAR')
               new_to = unicode('ASHB')
          elif this_to == unicode('RR'):
               # This must be northbound
               new_from = unicode('MCAR')
               new_to = unicode('ROCK')
               
     elif this_from == unicode('RR'):
          # Rockridge only has one option
          new_from = unicode('ROCK')
          if this_to == unicode('MA'):
               # This must be southbound
               new_to = unicode('MCAR_S')
          else:
               new_to = findMatch(this_to,namelist)
     else:
          new_from = findMatch(this_from,namelist)
          new_to = findMatch(this_to,namelist)
          
     # Route type here is manually forced to 1 since all of BART is identically 1
     fixed_final.append([new_from, new_to, 1, this_ridership])

print '... load the final data with 4-code names and the stopname map for reference'
db_cursor.execute('drop table if exists stop_name_map')
db_cursor.execute('create table stop_name_map (name_4code text, name_2code text)')
db_cursor.executemany('insert into stop_name_map values (?,?)',namelist)
conn.commit()

db_cursor.execute('drop table if exists real_ridership_segments_temp')
db_cursor.execute('create table real_ridership_segments_temp (from_stop text, to_stop text, route_type int, tot_ridership float)')
db_cursor.executemany('insert into real_ridership_segments_temp values (?,?,?,?)',fixed_final)
conn.commit()

print '... finalize'
db_cursor.execute('drop table if exists real_ridership_segments')
db_cursor.execute('create table real_ridership_segments as select from_stop, to_stop, route_type, sum(tot_ridership) as tot_ridership from real_ridership_segments_temp group by from_stop, to_stop, route_type')
conn.commit()

db_cursor.execute('drop table if exists real_ridership_segments_temp')
conn.commit()

# END Step 4
#----------


#----------
# Clean Up
db_cursor.execute('vacuum')
conn.commit()
conn.close

end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)