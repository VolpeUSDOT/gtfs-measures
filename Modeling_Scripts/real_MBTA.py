# Imports
import csv
import gtfsgen.gtfsloaders as gtfsloaders
import datawrap.wrapper_MBTA as wrapper_MBTA

# Configuration elements
DATA_FOLDER = 'TestAgency\\MBTA_GTFS\\'
ridershipfilename = 'ridership_estimate.csv'



print 'Preparing MBTA ridership data...'
with open((DATA_FOLDER + ridershipfilename),'rb') as csvfile:
    infile = csv.reader(csvfile,delimiter=',')
    counter = -1

    linkTempStorage = []
    stopRidershipData = []

    for row in infile:
        if counter >= 0:
            print counter
            
            # Get this row's info
            stopID = row[0]
            # stopDesc = row[1]     UNUSED
            routeID = row[2]
            boarding = float(row[3])
            alighting = float(row[4])
            loadOut = float(row[5])
            direction = row[6]
            depTime = row[7]
            dayOfWk = row[8]
            stopOrder = float(row[9])

            if counter == 0:
                # Very first row, put in the first elements
                linkTempStorage = [[routeID, dayOfWk, depTime, direction, [[stopID, stopOrder, loadOut]]]]
                stopRidershipData = [[stopID, dayOfWk, boarding, alighting, 1, routeID]]
            else:
                # Search for matching entries
                linkMatch = 0
                for item in linkTempStorage:
                    if item[0] == routeID and item[1] == dayOfWk and item[2] == depTime and item[3] == direction:
                        # Matching element, add info
                        item[4].append([stopID, stopOrder, loadOut])
                        linkMatch = 1
                        break
                if linkMatch == 0:
                    # No match, add a whole new element
                    linkTempStorage.append([routeID, dayOfWk, depTime, direction, [[stopID, stopOrder, loadOut]]])

                stopMatch = 0
                for item in stopRidershipData:
                    if item[0] == stopID and item[1] == dayOfWk:
                        # Matching element, add info
                        item[2] += boarding
                        item[3] += alighting
                        item[4] += 1
                        listedRoutes = item[5:]
                        if routeID not in listedRoutes:
                            item.extend([routeID])
                        stopMatch = 1
                        break
                if stopMatch == 0:
                    # No match, add a whole new element
                    stopRidershipData.append([stopID, dayOfWk, boarding, alighting, 1, routeID])
        counter += 1

# Adjust the link ridership data
linkRidershipData = []
for item in linkTempStorage:
    temp = sorted(item[4], key=lambda elem: elem[1])
    for r in range(len(temp)-1):
        row = [temp[r][0], temp[r+1][0], item[1], temp[r][2], item[0]]
        if len(linkRidershipData) == 0:
            linkRidershipData = [row]
        else:
            linkRidershipData.append(row)
        
print 'Writing out compiled data...'

# Load stop parents
stopParents = gtfsloaders.loadStopParentsFromCSV(('%sparentStops.csv' % GTFS_FOLDER))

# Adjust stations in both link and stop data
linkRidershipDataCondensed = []
target = 100
print 'Condensing Link Ridership'
for item in linkRidershipData:
    if len(linkRidershipDataCondensed) >= target:
        print '.',
        target += 100

    # Check through the list
    for possible in stopParents:
        if item[0] == possible[0]:
            item[0] = possible[1]
        if item[1] == possible[0]:
            item[1] = possible[1]

    # Checked, now add to a final list
    match = 0
    if len(linkRidershipDataCondensed) > 0:
        for comp in linkRidershipDataCondensed:
            if comp[0] == item[0] and comp[1] == item[1]:
                # Already in the list, combine
                match = 1
                comp[3] += item[3]
                if item[4] not in comp[4:]:
                    comp.extend([item[4]])
                break
    if match == 0:
        # Not already in the list, add it
        linkRidershipDataCondensed.append(item)
print '.'

stopRidershipDataCondensed = []
target = 100
print 'Condensing Link Ridership'
for item in stopRidershipData:
    if len(stopRidershipDataCondensed) >= target:
        print '.',
        target += 100
    
    # Check through the list
    for possible in stopParents:
        if item[0] == possible[0]:
            item[0] = possible[1]

    # Checked, now add to a final list
    match = 0
    if len(stopRidershipDataCondensed) > 0:
        for comp in stopRidershipDataCondensed:
            if comp[0] == item[0]:
                # Already in the list, combine
                match = 1
                comp[2] += item[2]
                comp[3] += item[3]
                comp[4] += item[4]
                for r in item[5:]:
                    if r not in comp[5:]:
                        comp.extend([r])
                break
    if match == 0:
        # Not already in the list, add it
        stopRidershipDataCondensed.append(item)
print '.'

# Go through the list of segment ridership and ensure that the stops are in order
with open(('%slinkRidership.csv' % GTFS_FOLDER),'wb') as csvfile:
    outfile = csv.writer(csvfile,delimiter=',')
    for item in linkRidershipDataCondensed:
        outfile.writerow(item)

with open(('%sstopRidership.csv' % GTFS_FOLDER),'wb') as csvfile:
    outfile = csv.writer(csvfile,delimiter=',')
    for item in stopRidershipDataCondensed:
        outfile.writerow(item)
