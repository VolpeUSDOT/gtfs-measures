#------------------------------------------------------------------------------
# Name:        Run Model Calibrations
#
# Purpose:     This function produces calibration statistics and exports 
#              results for a series of model layouts.  The models are all
#              simple linear combinations of all or a subset of the features
#              produced or compiled for each transit system.
#
# Author:      Stephen Zitzow-Childs
#
# Created:     Sprint 2017
# Updated:     7/19/2017
#
# Volpe National Transportation Systems Center
# United States Department of Transportation
#------------------------------------------------------------------------------


# This works for linear combination (and we can manually manipulate individual
# elements to create polynomial or exponential components in a limited way)
import datetime
import gtfs_model

start_time = datetime.datetime.now()
print 'Start at ',str(start_time)
print '======================================================================='


#print 'Model 01 - All Modes - Kitchen Sink'
#print '-----------------------------------'
#runname = 'calib_01_allmodes_kitchensink'
#route_type = [0,1,2,3]
#selected_features = ['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '-----------------------------------'
#print 'Model 02a - Bus Only - Kitchen Sink'
#print '-----------------------------------'
#runname = 'calib_02a_busonly_kitchensink'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '--------------------------------------------'
#print 'Model 02b - Bus Only - Significant Variables'
#print '--------------------------------------------'
#runname = 'calib_02b_busonly_significant'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x21','x23','x24','x26','x27','x29','x31','x32','x33','x34','x35','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '------------------------------------------'
#print 'Model 02c - Bus Only - Sig. Sig. Variables'
#print '------------------------------------------'
#runname = 'calib_02c_busonly_significant2'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x9','x11','x12','x13','x14','x15','x16','x17','x18','x19','x21','x23','x26','x27','x29','x31','x32','x34','x35','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '----------------------------------------------'
#print 'Model 02d - Bus Only - Sig. Sig. Sig. Variables'
#print '----------------------------------------------'
#runname = 'calib_02d_busonly_significant3'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x9','x11','x12','x13','x14','x15','x16','x17','x19','x21','x23','x26','x27','x29','x31','x32','x34','x35','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '-----------------------------------'
#print 'Model 03 - Rail Only - Kitchen Sink'
#print '-----------------------------------'
#runname = 'calib_03_railonly_kitchensink'
#route_type = [0,1,2]
#selected_features = ['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname)


#print '---------------------------------------------------------'
#print 'Model 04a - Bus Only - Exclude ValleyMetro - Kitchen Sink'
#print '---------------------------------------------------------'
#runname = 'calib_04a_busonly_excludeValleyMetro_kitchensink'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37']
#exclude = ['ValleyMetro']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname, exclude)


#print '----------------------------------------------------------'
#print 'Model 04b - Bus Only - Exclude ValleyMetro - Sig. Variables'
#print '----------------------------------------------------------'
#runname = 'calib_04b_busonly_excludeValleyMetro_significant'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x9','x11','x12','x13','x14','x15','x16','x17','x19','x20','x21','x22','x23','x24','x25','x27','x29','x30','x32','x33','x34','x36']
#exclude = ['ValleyMetro']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname, exclude)


#print '----------------------------------------------------------------'
#print 'Model 04c - Bus Only - Exclude ValleyMetro - Sig. Sig. Variables'
#print '----------------------------------------------------------------'
#runname = 'calib_04c_busonly_excludeValleyMetro_significant2'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x9','x11','x12','x13','x14','x15','x16','x17','x19','x20','x21','x22','x23','x25','x29','x32','x33','x34','x36']
#exclude = ['ValleyMetro']
#gtfs_model.calibrate_with_settings(route_type, selected_features, runname, exclude)


print '---------------------------------------------------------------------'
print 'Model 04d - Bus Only - Exclude ValleyMetro - Sig. Sig. Sig. Variables'
print '---------------------------------------------------------------------'
runname = 'calib_04d_busonly_excludeValleyMetro_significant3'
route_type = [3]
selected_features = ['x1','x2','x3','x4','x5','x9','x11','x12','x13','x14','x15','x16','x17','x19','x20','x21','x22','x23','x25','x29','x32','x33','x36']
exclude = ['ValleyMetro']
sqlresults = gtfs_model.calibrate_with_settings(route_type, selected_features, runname, exclude)


#print '---------------------------------------------------------------------'
#print 'Model 05 - Bus Only - Only ValleyMetro - All Variables'
#print '---------------------------------------------------------------------'
#runname = 'calib_05_busonly_onlyValleyMetro_kitchensink'
#route_type = [3]
#selected_features = ['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37']
#include = ['ValleyMetro']
#sqlresults = gtfs_model.calibrate_with_settings(route_type, selected_features, runname, [], include)


end_time = datetime.datetime.now()
total_time = end_time - start_time
print '======================================================================='
print '======================================================================='
print 'End at {}.  Total run time {}'.format(end_time, total_time)