import sqlite3
import write_shapefile

conn = sqlite3.connect('MBTA_gtfs.sqlite')
cursor_retrieve_coords = conn.cursor()
cursor_retrieve_coords.execute('select from_stop, stops1.stop_lat as from_lat, stops1.stop_lon as from_lon, to_stop, stops2.stop_lat as to_lat, stops2.stop_lon as to_lon from mbta_real_segments_unique inner join stops as stops1 on mbta_real_segments_unique.from_stop = stops1.stop_id inner join stops as stops2 on mbta_real_segments_unique.to_stop = stops2.stop_id')

segmentsinfo = cursor_retrieve_coords.fetchall()

write_shapefile.createSegments(segmentsinfo)