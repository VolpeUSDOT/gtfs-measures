#------------------------------------------------------------------------------
# Name:        Shortest Path
#
# Purpose:     This file implements the Djikstra shortest path algorithm for
#              pathing related to the BART system data. Certain BART elements
#              are hard coded.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

# This function finds the shortest path between two nodes and lists the resulting path and path length; algorith is djikstra
def rebuildpath(tracker, startnode, endnode):
     
     path = []
     
     # Find the correct endnode
     for node in tracker:
          if node[0] == endnode:
               # Found it - is it immediately preceded by the startnode?
               if node[2] == startnode:
                    # Easy - just the start to the end
                    path = [startnode, endnode]
               else:
                    # Not so easy, so find the path from the start to the immediate predecessor
                    path = rebuildpath(tracker, startnode, node[2])
                    # Now add the endnode
                    path.append(endnode)
     
     return (path)

def findpath(nodelist, segmentlist, startnode, endnode, maxdist, debug = False):
     
     path = []
     dist = maxdist
     
     # Some sanity checks
     if startnode not in nodelist:
          print 'Starting node not in domain'
          return (path, dist)
     if endnode not in nodelist:
          print 'Ending node not in domain'
          return (path, dist)
     
     # Prepare for shortest path algorithm
     # Tracker format:
     # Node id, distance, previous node id, visited (0 - no, 1 - yes)
     tracker = []
     for node in nodelist:
          if node == startnode:
               tracker.append([node, 0, node, 0])
          else:
               tracker.append([node, maxdist, '', 0])
     
     while True:
          # Find the closest node which has not been visited
          min_dist = maxdist
          all_visit = True
          for node in tracker:
               if node[3] == 0 and node[1] < min_dist:
                    min_dist = node[1]
                    all_visit = False
          
          # Debug
          if debug:
               print 'Step'
               print 'Minimum Distance to Examine: %f' % min_dist
               print 'Have we visited all nodes? ', all_visit
          
          # Select an origin node to examine
          for orignode in tracker:
               if orignode[3] == 0 and orignode[1] == min_dist:
                    # Analyze all outbound connections from this node
                    if debug:
                         print '---'
                         print '-Found node: %s' % orignode[0]
                    for segment in segmentlist:
                         if segment[0] == orignode[0]:
                              # Found a segment matching segment
                              if debug:
                                   print '-Found segment:'
                                   print '--From: %s' % segment[0]
                                   print '--To: %s' % segment[1]
                              for destnode in tracker:
                                   if destnode[0] == segment[1]:
                                        # Found the destination
                                        if debug:
                                             print '-Found destination: %s' % destnode[0]
                                        
                                        if destnode[1] > (min_dist + segment[2]):
                                             # Update shortest
                                             if debug:
                                                  print '--Old min dist: %f' % destnode[1]
                                             destnode[1] = (min_dist + segment[2])
                                             destnode[2] = segment[0]
                                             if debug:
                                                  print '--New min dist: %f' % destnode[1]
                                        elif debug:
                                             print '--No improvement'
                                        break
                    orignode[3] = 1
                              
          if all_visit:
               break
          
          break_from_endnode = False
          for node in tracker:
               if node[0] == endnode and node[3] == 1:
                    if debug:
                         print 'FINISH'
                         print 'Examined the destination node before all nodes complete'
                    break_from_endnode = True
                    break
          if break_from_endnode:
               break
          
     path = rebuildpath(tracker, startnode, endnode)
     
     for node in tracker:
          if node[0] == endnode:
               dist = node[1]
               break
     
     if debug:
          print path
          print dist
     
     return (path, dist, tracker)

# Testing function
import csv

nodelist = []
with open('C:/GTFS/BART/BARTstops.csv','rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     for row in infile:
          nodelist.append(row[0])

segmentlist = []
maxdist = 0
with open('C:/GTFS/BART/BARTonewaylinks.csv','rb') as csvfile:
     infile = csv.reader(csvfile,delimiter=',')
     for row in infile:
          segmentlist.append([row[0],row[1],int(row[2])])
          maxdist += int(row[2])
maxdist += 1

startnode = 'UC'
endnode = 'LF'

[path, dist, tracker] = findpath(nodelist, segmentlist, startnode, endnode, maxdist, False)
