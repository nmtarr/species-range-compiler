# GAP Range Map Compiler [DRAFT]
Combine species occurrence records, expert opinion, and extent of occurrence maps to create range maps.

## Purpose
The Gap Analysis Project developed range maps for 1,590 terrestrial vertebrate species by attributing a natural grid of spatial units (12 digit hydrologic unit code (HUC) vector data layer) with each species' status regarding known presence, seasonal presence, use for reproduction, and origin (i.e., is it native?).  

The abundance of wildlife occurrence datasets that are currently accessible represent an opportunity to update some features of the GAP range data.  However, this task is more complex than it may seem at first consideration given errors and uncertainties in occurrence data and the lack of absence records.

## Framework
The wildlife-wrangler code creates databases of species occurrence records that passed through user-defined and documented criteria.  This range compilation code connects to such databases and uses the records to evaluate GAP's 12 digit HUC-based range maps, which are downloaded from ScienceBase. 

## Steps
1. Identify the taxon concept of interest, as well as any associated taxon concepts from the time
period of interest. 
    a. P:/Proj3/USGap/Vert/DBase/taxa_map.sqlite

2. Acquire and curate occurrence records for all relevant taxa with wildlife-wrangler.
    a. Copy and use wildlife-wrangler/Query_TEMPLATE.ipynb

3. Review the retained records to check for duplication among taxon concepts and problematic information in the remarks columns.  Weight the occurrence records with a script and/or interactive tools in QGIS.
    a. Copy and modify ./xXXXXx0-weighting_TEMPLATE.py

4. Set range compilation parameters
    a. P:/Proj3/USGap/Vert/DBase/range-parameters.sqlite

5. Compile the range 
    a. Edit and run ./GAP-range-compiler.py
    b. Or edit and run run_process.sh

6. Generate a change summary (optional)
    a. With ./range-change-summary.py
    b. Or edit and run run_process.sh

7. Run tests of results (optional)
    a. ./Tests/GAP-range-tests.py
    b. Or edit and run run_process.sh

8. Draw up in QGIS 
    a. ./QGISTools/load range.py

9. Edit by recording your opinion in QGIS
    a. ./QGISTools/register_opinion.py (inserts into P:/Proj3/USGap/Vert/DBase/range_opinions.sqlite)
    b. readjust occurrence records weights.

10. Repeat steps 4 through 9 as necessary.

## Objectives and Criteria
This framework is designed to meet several important criteria in order to provide summaries that can be interpreted at face value with high confidence (i.e. with minimized human effort).

* Open source -- processes are coded in Python 3 and SQL and use sqlite3, which comes with Python 3, for spatial queries.

* Automation -- the volume of data and species involved necessitates the processes be automated to the full extent possible. Automation also reduces subjectivity in decision making, enables thorough documentation, and ensures repeatability.

* High confidence -- data, filters, and weighting should be used (in wildlife-wrangler) that enable high confidence in results.

* Detailed parameterization -- range compilation can be parameterized on a per-species and per-event basis. Rules do not have to be applied generally across large numbers of species or compilations.

* Transparency through documentation -- decisions about how to structure compilation are documented in an sql database.  The inputs for the compilations are sqlite databases of occurrence records from the wildlife-wrangler code.

* Misidentifications -- even professionals are not perfect; so citizen scientists surely mistakenly identify species.  Presence-only data do not directly record absence, so false-positives are the potential issue.  They can expand and distort range delineations and falsely validate GAP range maps.  A simple way to account for them is to employ a threshold summed weight for a region (i.e., a HUC) before the species is determined to be present there.

* Misidentifications and low-quality records -- even professionals are not perfect; so citizen scientists surely mistakenly identify species.  Presence-only data do not directly record absence, so false-positives are the issue here.  They can expand and distort range delineations and falsely validate GAP range maps.  A simple way to account for them is to apply weights to the individual occurrence records and set a minimum for the summed weight for a target region (i.e., a HUC) before the species is determined to be present there.  See the notebook "Method for attributing species to subregions."


## Inputs
Occurrence data is pulled from occurrence record databases that were created with wildlife-wrangler.  Range compilation criteria are stored in "ranges-records.sqlite".  Additionally, the GAP 12 digit HUC ancillary layer is needed, and although it is available on ScienceBase.gov as a geodatabase, a different format is needed.  Hopefully a shapefile version can be uploaded there eventually.

## Outputs
On a per-species basis
* A database of GAP range information from which an updated range evaluation shapefile can be created.


## Contributing
Git workflow - the master branch has the most current, working code, but should
be treated as a template by all users, whereby they create their own branches
to use for running/application and update those personal branches with updates
by merging the master branch into the personal branch periodically.  For 
development, users can create feature branches and push those to the server
for others to review.  The server is on the p:/ drive.

## Constraints
None at this time

## Dependencies
Python 3 and numerous packages including sqlite3 with the spatialite extension.  An environment can be created from the ENVIRONMENT.yml file included in this repository.  This code relies upon the wildlife-wrangler code.  

## Code
All code is included in this repository.  Runtimes of discrete tasks made grouping code into separate scripts preferable.

## Status
Removing occurrence record download and filter capabilities to make this repo dependent upon but not redundant with wildlife-wrangler
