# Multimodal GTFS Measures: Ridership Modeling Scripts

28 August 2017

## Introduction

This series of scripts use real ridership data provided by several transit agencies across the country to calibrate a predictive model for ridership.  The predictive model is based on service characteristics and network parameters developed from a GTFS feed, and additional socio-demographic parameters which are available nationwide.  The scripts deal with the significant variations between ridership data structures and prepare ridership information in a standardized format for analysis.  All analysis is focused on the 'segment' level.  A segment represents the portion of a transit trip from one stop to the next adjacent stop - transit riders cannot board or alight in the middle of a segment.  Segments are also treated as directional - ridership from stop A to stop B is distinct from riderhsip from stop B to stop A.

Modes are treated separately in this analysis, using the categories as defined in the [GTFS specification](https://developers.google.com/transit/gtfs/reference/).

## Script Descriptions

The majority of the modeling scripts are labeled in steps from 00 to 09.  Most steps rely upon inputs from previous steps, although some are independent.                                                                                      
                                                                                      
### Step 00: Load GTFS

Prior to loading real ridership data, the GTFS feed for each agency of interest is imported.  The pygtfs package includes importation routines which handle the majority of loading without issue.  However, due to a type mismatch, some fields in certain tables are stripped on importation.  A separate import file is included to correct those importation errors.  All GTFS data is imported and written to a SQLite database for manipulation.

### Step 01: Assess GTFS

Once the GTFS feeds are imported into SQLite, service characteristics which impact ridership are extracted.  These include frequency and mode of service and number of routes serving each segment.  Values are assembled for a 'typical' day which matches with the input ridership data.

### Step 02: Import Real Ridership Data

This step includes multiple scripts, each of which handles the particular formatting issues associated with ridership data from one agency.  Ridership data varied in aggregation in time - some data was provided on a route-run basis, with every transit vehicle on the system having separate data across an entire day.  Other data was aggregated up to the day total.  Most data was provided as segment ridership, although boarding and alighting counts were also provided in many cases on a stop-by-stop basis.  Data provided by BART is in the form of an origin-destination matrix which was routed on to the BART network using a Dijkstra shortest path algorithm implementation (shortest_path.py).  Ridership is ultimately reduced to segment-level day total ridership by mode.
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
### Step 03: Add Socio-Demographic Features

Several additional data importation steps are taken to read nation-level dataset information and associate them with transit stops.  Economic, housing, vehicle ownership, and other data are extracted from datasets produced by the U.S. Census Bureau.  Since most of these data are provided at the block or block-group level, a quarter-mile catchment area around each transit stop was used to produce an averaged value for each measure.  The resulting stop-assocated data are the inputs to these scripts which load them into the SQLite database.
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
### Step 04: Conflate Statistics

After loading all previous datasets into the SQLite database and aggregating to the segment level, the various tables must be conflated to a single lookup table which can be used for modeling and analysis.  Segments without ridership data or segments with ridership data but no associated service characteristics (due to some mismatch in naming or similar error) are dropped from the final conflated dataset.  Socio-demographic data are associated to each ridership segment based on the origin stop.

### Step 05: Join to All

This step performs a union on the data sets provided by multiple agencies to produce a single master SQLite database which can be queried for analysis.

### Step 06: Run Model Calibration

With a single unified database of segments, ridership, service characteristics, and socio-demographic data, a least squares model estimation is performed on linear combination model.  A linear combination model is in the form:

y = (b<sub>1</sub> * x<sub>1</sub>) + (b<sub>2</sub> * x<sub>2</sub>) + ... + (b<sub>N</sub> * x<sub>N</sub>) + C

In this form, every input variable has a single coefficient and the overall fit has a single added constant.  

A summary csv is produced including the calibration coefficients.

### Step 07: Estimate Ridership from Model

With a fitted model from Step 06, ridership for an agency can be estimated.  The input parameters must be specified in a special format used by the optimization routine.  Essentially, the form of the linear combination model must be specified (which input parameters are used).  With that, the appropriate fields from the conflated statistics are used to estimate ridership by segment.  A number of predefined model formats are included within a separate script called by the model estimator.  Estimated ridership is written to the SQLite database.

### Step 08: Produce Ridership CSVs

This script produces a csv file with real ridership for a given agency.  If a single origin-destination transit segment has multiple physical paths on the road network, ridership is split across the routes according to frequency of service.  Headers are included.

### Step 09: Produce Estimated CSVs

This script does the same process as Step 08 for estimated ridership for a given agency.

### Other Files

Several other files are included which have supporting functions for plotting, runtime output, etc.  Examine each for descriptions inline.

## Possible Improvements

-   Rewrite GTFS import scripts for efficiency or submit correction requests to pygtfs developers

-   Add new model forms beyond a linear combination model

-   Examine the significance and covariance of input parameters

-   Streamline overall code; there is some redundancy, especially in the data loading, which may be simplified

-   Extend visualization/output routines to produce more/better figures and tables