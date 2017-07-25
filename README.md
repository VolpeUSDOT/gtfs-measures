# GTFS for Estimating Transit Ridership and Supporting Multimodal Performance Measures

This repository contains the code that the U.S. DOT Volpe Center and Office of the U.S DOT Undersecretary for Policy developed for their proof-of-concept project exploring how General Transit Feed Specification (GTFS) public schedule data, widely available from transit agencies and now through the National Transit Map, could be used to estimate road segment-level transit ridership and support multimodal performance measures.

## Usage
This code estimates segment-level transit service characteristics (e.g. frequency, ridership at the level of individual road segments) based on GTFS feeds and other inputs.

## Link to Associated Report

### Components
- Modeling Scripts: Ingests GTFS, calibration ridership data, and predictive inputs to build a model for frequency and ridership.
- Spatial Scripts: Prepares a segment-based network of roads that can be attached to transit service and estimated characteristics from the modeling scripts.

## Code Dependencies
- [Python](https://www.python.org/)
- [ArcPy](http://pro.arcgis.com/en/pro-app/arcpy/get-started/what-is-arcpy-.htm) (part of ArcGIS)
- [pygtfs](https://github.com/jarondl/pygtfs)
- [sqlite3](https://www.sqlite.org/)

## Data Dependencies
- GTFS Data from the [U.S. DOT National Transit Map](https://www.rita.dot.gov/bts/ntm)
- Federal Highway Administration's [All Roads Network of Linear Referenced Data](https://www.fhwa.dot.gov/policyinformation/hpms/arnold.cfm) (ARNOLD). (Note: the full geospatial data for this network is not yet available to the public. However, the project code could be modified to use state-specific roads data or other national roads data)
- For ridership estimation: measured transit ridership data to calibrate the estimation model. While this project also used additional ridership data that transit agencies provided directly, the following agencies provide segment-level ridership as open data to the public:
 - [Twin Cities Metro Transit](ftp://ftp.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_metc/trans_stop_boardings_alightings/metadata/metadata.html)
 - [Bay Area Rapid Transit](https://www.bart.gov/about/reports/ridership)
 - For ridership estimation: Various data that may predict ridership from the U.S. Census and other national inputs as described in this project's full report. Code is designed to be modified to allow for other predictive datasets to be used.


## Documentation
This code is primarily documented through in-line comments, with some additional documentation listed below:
- [GTFS to Road Network (ARNOLD) Snapping](https://github.com/VolpeUSDOT/gtfs-measures/blob/master/docs/GTFS_Script_Documentation.md)

## Getting involved
This public-domain code was developed as part of a proof-of-concept project, and we encourage transit agencies, state DOTs, or other interested parties to consider how these tools to estimate ridership and calculate multimodal performance measures might be useful to them. We also welcome contributions back into this repoistory as a building block for peers. See [LICENSE](LICENSE),  [CONTRIBUTING](CONTRIBUTING.md), and [TERMS](TERMS.md).

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [U.S. Government Source Code Policy](https://sourcecode.cio.gov/)
