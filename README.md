# GTFS for Estimating Transit Ridership and Supporting Multimodal Performance Measures

This repository contains the code that the U.S. DOT Volpe Center and Office of the U.S DOT Undersecretary for Policy developed for their proof-of-concept project exploring how General Transit Feed Specification (GTFS) public schedule data, widely available from transit agencies and now through the National Transit Map, could be used to estimate road segment-level transit ridership and support multimodal performance measures.

## Link to Associated Report

The [full report](https://rosap.ntl.bts.gov/view/dot/34279) is available from the National Transportation Library.

## Usage
This code estimates segment-level transit service characteristics (e.g. frequency, ridership at the level of individual road segments) based on GTFS feeds and other inputs, mapping the results spatially. For bus transit, it attaches these characteristics to the underlying road network for easy comparison to vehicular Average Annual Daily Traffic (AADT) data available from states, other local agencies, and the [Federal Highway Administration](https://www.fhwa.dot.gov/policyinformation/hpms.cfm). We encourage transit agencies, states, or other agencies to use, adapt, and contribute to this code if they find it useful in understanding and managing their transportation system.

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
  - [Twin Cities Metro Transit](https://gisdata.mn.gov/dataset/us-mn-state-metc-trans-stop-boardings-alightings)
  - [Bay Area Rapid Transit](https://www.bart.gov/about/reports/ridership)
- For ridership estimation: Various data that may predict ridership from the U.S. Census and other national inputs as described in this project's full report. Code is designed to be modified to allow for other predictive datasets to be used.


## Documentation
This code is primarily documented through in-line comments, with some additional documentation listed below:
- [GTFS to Road Network (ARNOLD) Snapping](https://github.com/VolpeUSDOT/gtfs-measures/blob/master/docs/GTFS_Script_Documentation.md)
- [Ridership Modeling](https://github.com/VolpeUSDOT/gtfs-measures/blob/master/docs/GTFS_Model_Scripts_Documentation.md)

## Getting involved
This public-domain code was developed as part of a proof-of-concept project, and we encourage transit agencies, state DOTs, or other interested parties to consider how these tools to estimate ridership and calculate multimodal performance measures might be useful to them. We also welcome contributions back into this repository as a building block for peers. See [LICENSE](LICENSE),  [CONTRIBUTING](CONTRIBUTING.md), and [TERMS](TERMS.md).

If you have questions about the code or the project, feel free to open an Issue on this repository or reach out to the contacts noted in the full report linked above.

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [U.S. Government Source Code Policy](https://sourcecode.cio.gov/)
