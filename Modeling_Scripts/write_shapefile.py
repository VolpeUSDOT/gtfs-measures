# Testing function to interface data with ArcGIS

import arcpy

def createSegments(segmentcoords):
     # Form of segmentcoords:
          # 0 - from_stop id, 1 - from_stop lat, 2 - from_stop lon
          # 3 - to_stop id, 4 - to_stop lat, 5 - to_stop lat
     print 'Create feature class'
     arcpy.CreateFeatureclass_management('C:\\GTFS\\','segments.shp','POLYLINE')
     arcpy.AddField_management(r'segments.shp','from_stop','TEXT')
     arcpy.AddField_management(r'segments.shp','to_stop','TEXT')
     
     fieldnames = ('SHAPE@','from_stop','to_stop')
     with arcpy.da.InsertCursor(r'segments.shp',fieldnames) as cursor:
         print 'Insert coordinate pairs'
         for r in range(len(segmentcoords)):
             from_stop_id =   segmentcoords[0]
             from_stop_lat =  segmentcoords[1]
             from_stop_lon =  segmentcoords[2]
             to_stop_id =     segmentcoords[3]
             to_stop_lat =    segmentcoords[4]
             to_stop_lon =    segmentcoords[5]
             
             # Set X and Y for start and end points
             array = arcpy.Array([arcpy.Point(from_stop_lat,from_stop_lon),arcpy.Point(to_stop_lat,to_stop_lon)])
                                 
             # Create a Polyline object based on the array of points
             polyline = arcpy.Polyline(array)
     
             # Add it
             cursor.insertRow((polyline,from_stop_id,to_stop_id))