#------------------------------------------------------------------------------
# Name:        GTFSFreq
#
# Purpose:     This file includes two functions which tally stop-to-stop and 
#              stop-level frequency, accounting for route shape information for
#              stop segments.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Winter 2016
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------

import myprint

#---
# Function to parse through a list of segment information in the form:
#    from_stop, to_stop, shape_id, route_type, route_id
# 
# Output is unique lists of segments with frequency and number of routes based on:
#    from_stop, to_stop, route_type      and
#    from_stop, to_stop, route_type, shape_id
def calculateSegmentFrequency(all_segments):
     prev_from = []
     prev_to = []
     prev_shape = []
     prev_type = []
     temp_from_to_type_shape_freq = 0
     temp_from_to_type_shape_rts = []
     temp_from_to_type_freq = 0
     temp_from_to_type_rts = []
     from_to_type_shape_results = []
     from_to_type_results = []
     
     for this_seg in all_segments:
          # 0 - from_stop, 1 - to_stop, 2 - shape_id, 3 - route_type, 4 - route_id
          # 5 - frequency adjustment
          if this_seg[0] == prev_from and this_seg[1] == prev_to and this_seg[3] == prev_type:
               # Matches the previous from,to,type
               temp_from_to_type_freq += this_seg[5]
               if this_seg[4] not in temp_from_to_type_rts:
                    temp_from_to_type_rts.append(this_seg[4])
               
               if this_seg[2] == prev_shape:
                    # Shape also matches
                    temp_from_to_type_shape_freq += this_seg[5]
                    if this_seg[4] not in temp_from_to_type_shape_rts:
                         temp_from_to_type_shape_rts.append(this_seg[4])
               else:
                    # from,to,type matches but NOT shape... store and start a new temp
                    from_to_type_shape_results.append(tuple([prev_from, prev_to, prev_type, prev_shape, temp_from_to_type_shape_freq, len(temp_from_to_type_shape_rts)]))
                    temp_from_to_type_shape_freq = this_seg[5]
                    temp_from_to_type_shape_rts = [this_seg[4]]
                    prev_shape = this_seg[2]
          else:
               # Does not match previous from,to,type
               if prev_from != []:
                    # Store if it isn't the first step
                    from_to_type_shape_results.append(tuple([prev_from, prev_to, prev_type, prev_shape, temp_from_to_type_shape_freq, len(temp_from_to_type_shape_rts)]))
                    from_to_type_results.append(tuple([prev_from, prev_to, prev_type, temp_from_to_type_freq, len(temp_from_to_type_rts)]))
               
               # Update the information
               prev_from = this_seg[0]
               prev_to = this_seg[1]
               prev_shape = this_seg[2]
               prev_type = this_seg[3]
               temp_from_to_type_shape_freq = this_seg[5]
               temp_from_to_type_freq = this_seg[5]
               temp_from_to_type_shape_rts = [this_seg[4]]
               temp_from_to_type_rts = [this_seg[4]]
          
          if len(from_to_type_shape_results) % 1000 == 0:
               myprint.update()
     myprint.final()
     
     return (from_to_type_results, from_to_type_shape_results)

#---
# Function to parse through a list of segment information in the form:
#    from_stop, to_stop, shape_id, route_type, route_id
# 
# Output is unique lists of segments with frequency and number of routes based on:
#    from_stop, to_stop, route_type      and
#    from_stop, to_stop, route_type, shape_id
def calculateStopFrequency(all_stops):
     stop_freq = []
     
     prev_stop = []
     temp_stop_freq = 0
     temp_stop_rts = []
     temp_stop_types = []
     intermodal = 0
     
     for this_stop in all_stops:
          # 0 - from_stop, 1 - route_type, 2 - route_id, 3 - freq_adj
          if prev_stop == this_stop[0]:
               # Same stop, add information
               temp_stop_freq += this_stop[3]
               if this_stop[1] not in temp_stop_types:
                    temp_stop_types.append(this_stop[1])
                    intermodal = 1
               if this_stop[2] not in temp_stop_rts:
                    temp_stop_rts.append(this_stop[2])
          else:
               # Doesn't match the previous stop
               if prev_stop != []:
                    stop_freq.append(tuple([prev_stop,temp_stop_freq,len(temp_stop_rts),len(temp_stop_types),intermodal]))
               # Update
               prev_stop = this_stop[0]
               temp_stop_freq = this_stop[3]
               temp_stop_types = [this_stop[1]]
               temp_stop_rts = [this_stop[2]]
               intermodal = 0
          
          if len(stop_freq) % 1000 == 0:
               myprint.update()
     myprint.final()
     
     return (stop_freq)