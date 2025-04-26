"""
Title: The USGS Gap Analysis Project Transparent Range Compiler
Author: Nathan Tarr; nmtarr@ncsu.edu
Date: July 1, 2022

Description: Compiles a presence range map using species occurrence records and
    a polygon grid such as the GAP 12-digit HUCS along with predefined
    parameters and rules for compilation that are stored in a parameters
    database.

Notes:
- Run from command line with "python GAP-range-compiler.py" to avoid errors
    related to multicore processing and Ipython.
- Input must come from the wildlife-wrangler.
- Attempts to put the core functions into an importable module have not been
    successful due to limitations surrounding multicoreprocessing.
"""
###############################################################################
###################   SET VARIABLES IN THIS SECTION   #########################
###############################################################################
import sys
import os
#-----------------------  Species and variables  ------------------------------
# Set variables for the species and season
task_name = sys.argv[1]  # a short, memorable name to use for file names etc
gap_id = sys.argv[2]
task_id = sys.argv[3]
seasons = sys.argv[4].split(",")
author = sys.argv[5]
code_version = "0.1.3"

#---------------------------  Paths to use  -----------------------------------
workDir = sys.argv[6]  # path to the working directory
# List occurrence record databases in order of precendence.  Records from the
# first will take presedence if duplicates arise.
ww_output = tuple(sys.argv[7].split(","))
 
# Point to code directories
codeDir = sys.argv[8]
gapproductionDir = sys.argv[9]
wrangler_path = sys.argv[10]

# Name the output database
task_db = workDir + "/" + gap_id + task_id + ".sqlite"

# Make a temp dir in the working directory if it doesn't already exist
tmpDir = workDir + "temp/"
if not os.path.exists(tmpDir):
    os.makedirs(tmpDir)

# Set the path to the parameters and grid databases
parameters_db = "REPLACETHIS/Vert/DBase/range-parameters.sqlite"
grid_db = sys.argv[11]

periods = ((2001, 2005), (2006, 2010), (2011, 2015), (2016, 2020), (2021, 2025))


###############################################################################
###################   DO NOT CHANGE THE CODE BELOW   ##########################
###############################################################################
# Universal variables
RangeCodesDict2020 = {"Presence": {1: "Confirmed present",
                                   2: "Likely present",
                                   3: "Suspected present",
                                   4: "Suspected absent",
                                   5: "Likely absent"},
                      "Season": {"y": "Year-round",
                                 "s": "Summer",
                                 "w": "Winter"}}

RangeCodesDict2001 = {"Presence": {1: "Known/extant",
                                   2: "Possibly present",
                                   3: "Potential for presence",
                                   4: "Extirpated/historical presence",
                                   5: "Extirpated purposely (applies to introduced species only)",
                                   6: "Occurs on indicated island chain",
                                   7: "Unknown"},
                "Origin": {1: "Native", 2: "Introduced", 3: "Either introduced or native",
                           4: "Reintroduced", 5: "Either introduced or reintroduced",
                           6: "Vagrant", 7: "Unknown"},
                "Reproduction": {1: "Breeding", 2: "Nonbreeding",
                                 3: "Both breeding and nonbreeding", 7: "Unknown"},
                 "Season": {1: "Year-round", 2: "Migratory", 3: "Winter", 4: "Summer",
                            5: "Passage migrant or wanderer", 6: "Seasonal permanence uncertain",
                            7: "Unknown", 8: "Vagrant"}}

# ------------------------------------------------------------- Import packages
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import sqlite3
import pandas as pd
import multiprocessing as mp
sys.path.append(wrangler_path)
import wrangler_functions as wf
os.chdir(workDir)
from datetime import datetime
sys.path.append(gapproductionDir)
from gapproduction import database

#  ------------------------------------------------------------- Get parameters
def get_parameters():
    """
    Retrieves the compilation parameters from the parameters database
    """
    cursor, conn = spatialite(parameters_db)
    months = cursor.execute("""SELECT months FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()
    months = tuple([x.strip().zfill(2) for x in months[0].split(',')])
    years = cursor.execute("""SELECT years FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()
    years = tuple([x.strip() for x in years[0].split(',')])
    error_tolerance = cursor.execute("""SELECT error_tolerance FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()
    creator = cursor.execute("""SELECT creator FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()
    extralimital_m = cursor.execute("""SELECT extralimital_cutoff_m FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()
    use_GAPv1 = cursor.execute("""SELECT use_GAPv1 FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()[0]
    if use_GAPv1 == 'yes':
        use_GAPv1 = True
    else:
        use_GAPv1 = False
    use_opinions = cursor.execute("""SELECT use_opinion FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()[0]
    if use_opinions == 'yes':
        use_opinions = True
    else:
        use_opinions = False
    use_observations = cursor.execute("""SELECT use_observations FROM tasks WHERE task_id = ? AND species_id = ?;""", (task_id, gap_id)).fetchone()[0]
    if use_observations == 'yes':
        use_observations = True
    else:
        use_observations = False

    conn.close()

    return (years, months, error_tolerance, creator, extralimital_m, use_GAPv1, 
           use_observations, use_opinions)

# ----------------------------------------------------- Prep occurrence records
def occurrence_records(database, out_file):
    """
    Creates a shapefile of the species occurrence records (wildlife wrangler
        output) that can be loaded into the range database.

    PARAMETERS
    ----------
    out_file -- path and name for output file, without a file extension
    database -- path to the wildlife wrangler occurrence records output
    use_observations -- True if you want to use the wildlife wrangler output.
    """
    try:
        timestamp = datetime.now()

        # Get records, prep for use below
        df1 = wf.spatial_output(database=database, make_file=False,
                                mode="footprint", output_file=None,
                                epsg=5070)

        # Delete some columns
        df2 = df1.filter(["index", "taxon_id", "record_id", "eventDate",
                        "weight", "weight_notes", "geometry"], axis=1)

        # Save as shapefile
        df2.to_file(out_file, driver='ESRI Shapefile', encoding="UTF-8")
        print("Generated occurrence records shapefile: ",
            str(datetime.now()-timestamp))
    except Exception as e:
        print("Couldn't create an occurrence records shapefile", e)

# --------------------------------------------- Connect to sqlite w/ spatialite
def spatialite(db=":memory:"):
    """
    Creates a connection and cursor for sqlite db and enables spatialite
        extension and shapefile functions.  Defaults to in-memory database.

    (db) --> cursor, connection

    PARAMETERS
    ----------
    db -- path to the db you want to create or connect to.
    """
    import os
    import sqlite3
    import platform

    # Environment variables need to be handled
    if platform.system() == 'Windows':
        os.environ['PATH'] = os.environ['PATH'] + ';' + 'C:/Spatialite'
        os.environ['SPATIALITE_SECURITY'] = 'relaxed'# DOES THIS NEED TO BE RUN BEFORE EVERY CONNECTION????? ?NOT WORKING  ???????????

    if platform.system() == 'Darwin':  # DOES THIS NEED TO BE RUN BEFORE EVERY CONNECTION?????????????????
        #os.putenv('SPATIALITE_SECURITY', 'relaxed')
        os.environ['SPATIALITE_SECURITY'] = 'relaxed'
    connection = sqlite3.connect(db, check_same_thread=False)
    cursor = connection.cursor()
    os.putenv('SPATIALITE_SECURITY', 'relaxed')
    connection.enable_load_extension(True)
    cursor.execute('SELECT load_extension("mod_spatialite");')
    # Use write-ahead log mod to allow parallelism
    cursor.executescript("""PRAGMA synchronous=OFF;
                            PRAGMA journal_mode=WAL;""")
    connection.commit()
    return cursor, connection

#  --------------------------------------------------------- Download GAP range
def download_GAP_range_CONUS2001v1(gap_id, toDir):
    """
    Downloads GAP Range CONUS 2001 v1 file and returns path to the unzipped
    file.  NOTE: doesn't include extension in returned path so that you can
    specify if you want csv or shp or xml when you use the path.

    PARAMETERS
    ----------
    gap_id -- gap species code. For example, 'bAMROx'
    toDir -- path to the directory where you want the file to be downloaded.

    RETURNS
    -------
    sb_success -- boolean indicating whether the download was successful
    rng_zip -- path to the unzipped file
    """
    import sciencebasepy
    import zipfile
    from datetime import datetime
    time0 = datetime.now()

    # Connect
    sb = sciencebasepy.SbSession()

    try:
        # Search for gap range item in ScienceBase
        gap_id = gap_id[0] + gap_id[1:5].upper() + gap_id[5]
        item_search = '{0}_CONUS_2001v1 Range Map'.format(gap_id)
        items = sb.find_items_by_any_text(item_search)

        # Get a public item.  No need to log in.
        rng =  items['items'][0]['id']
        item_json = sb.get_item(rng)
        get_files = sb.get_item_files(item_json, toDir)

        # Unzip
        rng_zip = toDir + item_json['files'][0]['name']
        zip_ref = zipfile.ZipFile(rng_zip, 'r')
        zip_ref.extractall(toDir)
        zip_ref.close()

        # Return path to range file without extension
        print("Downloaded GAP 2001v1 range map: " + str(datetime.now() - time0))
        sb_success = True
        return sb_success, rng_zip.replace('.zip', '')

    except:
        sb_success = False
        print("No data available from ScienceBase")
        return sb_success, "No Zip"

# ------------------------------------------------------------- Query GAP range
def get_GAP_range_database_INCOMPLETE(species_code : str, db : str,
                           toDir : str) -> tuple:
    '''
    THIS FUNCTION WILL NOT WORK IN THE COMPILER PROCESS BECAUSE IT MAY PRODUCE
    2016 RANGE CSV FILE THAT WON'T BE ACCOMODATED.

    ADDITIONAL ISSUES NEED TO BE ADDRESSED BEFORE USING THAT RELATE TO
    TO PROVENANCE AND THE INCORPORATION OF EDITS TO THE GAP DATABASE THAT WERE
    MADE OUTSIDE OF THIS FRAMEWORK/PROCESS.

    Reads in a species' range from a GAP database and returns a dataframe.

    PARAMETERS
    ----------
    species_code : GAP 6-character species code to get HUC range for
        Example: 'aBESAx'
    db : GAP vertebrate database version name
        Example: 'GapVert_48_2016_test'
    toDir : path to the directory where you want the file to be downloaded.

    RETURNS
    -------
    success : Boolean indicating whether the function ran successfully.
    out_csv : Path to the output file (csv).

    '''
    try:
        year = db[-4:]

        # Connect to the database
        sppCur, sppConn = database.ConnectDB(db)
        
        # Build an SQL statement using a species code
        # Limit the HUC codes to only CONUS - i.e. < 190000000000                
        sql = """SELECT t.strHUC12RNG, t.intGapOrigin, t.intGapPres,
                        t.intGapRepro, t.intGapSeas
                    FROM dbo.tblRanges as t
                    WHERE (t.strUC = ?) AND (t.strHUC12RNG < 190000000000)
                    ;"""
                    
        # Set query results as a pandas dataframe
        df = pd.read_sql(sql, sppConn, params=['mAMMAx'])
        
        # Add string version columns of each column with "GAP" in the name
        #for col in df.columns:
        #    if "intGAP" in col:
        #        df["strGAP" + col[6:]] = [str(x) for x in df[col]]

        # Delete cursor and close db connection
        del sppCur
        sppConn.close()

        # Save to csv file
        out_csv = toDir + species_code + f"_CONUS_RANGE_{year}v1.csv"
        df.to_csv(out_csv, index=False)

        success = True
        return success, out_csv
    except:
        success = False
        print("!!! Unable to get range from GAP database !!!")
        return success, "FAILED"

#  ----------------------------------------------- Make database for processing
def make_range_db(task_db, gap_id, inDir, workDir, grid_db, sb_success,
                  seasons, parameters_db=parameters_db, use_v1=True, 
                  use_observations=True):
    """
    Builds an sqlite database in which to store range information.
    Creates tables for GAP range (full and presence column only with geometry).

    PARAMETERS
    ---------
    task_db -- name of database to create for task
    gap_id -- gap species code. For example, 'bAMROx'
    grid_db -- path to GAP's 12 digit hucs in sqlite/spatialite format
    in -- project's input directory
    workDir -- output directory for this repo
    sb_success -- returned variable from 2001 range download function
    parameters_db -- path to the parameters database
    use_v1 -- boolean indicating whether to use the 2001 range
    use_observations -- boolean indicating whether to use the occurrence records
    use_opinion -- boolean indicating whether to use the expert opinion
    """
    import sqlite3
    import pandas as pd
    import os
    from datetime import datetime
    time0 = datetime.now()

    # Delete db if it exists
    if os.path.exists(task_db):
        os.remove(task_db)

    # Create the database
    cursorQ, conn = spatialite(task_db)

    cursorQ.execute('SELECT InitSpatialMetaData(1);')
    print("Check Spatial MetaData: -------------------")
    print(cursorQ.execute('SELECT checkSpatialMetaData();').fetchall())


    ########################################################## COMPILATION INFO
    """Create a table documenting author,
     date, code version, comments.  Build
    if from the parameters database record."""
    conP = sqlite3.connect(parameters_db)

    # Get parameters from the parameters database
    pardf = pd.read_sql("""SELECT * FROM tasks 
                           WHERE task_id = ? AND species_id = ?;""",
                        conP, params=[task_id, gap_id])

    # Add column with who ran the code
    pardf['who_ran'] = author

    # Add column with code version
    pardf['code_version'] = code_version

    # Add column with date
    pardf['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write to task database
    pardf.to_sql('compilation_info', conn, if_exists='replace', index=False)


    ########################################################## ADD 2001v1 RANGE
    csvfile = tmpDir + gap_id + "_CONUS_RANGE_2001v1.csv"
    if sb_success == True:
        sp_range = pd.read_csv(csvfile, dtype={'strHUC12RNG':str})
        sp_range.to_sql('range_2001v1', conn, if_exists='replace', index=False,
                        index_label="strHUC12RNG")
        conn.commit() # Commit and close here, reopen connection or else code throws errors.
        conn.close()

        # Rename columns and drop some too.
        sql1 = """
        ALTER TABLE range_2001v1 RENAME TO garbage;

        CREATE TABLE range_2001v1 AS SELECT strHUC12RNG,
                                    intGapOrigin AS intGAPOrigin,
                                    intGapPres AS intGAPPresence,
                                    intGapRepro AS intGAPReproduction,
                                    intGapSeas AS intGAPSeason,
                                    Origin AS strGAPOrigin,
                                    Presence AS strGAPPresence,
                                    Reproduction AS strGAPReproduction,
                                    Season AS strGAPSeason
                            FROM garbage;
        DROP TABLE garbage;

        /*  Set a primary key -- this is cumbersome code due to sqlite3/pandas limitations.*/
        PRAGMA foreign_keys=off;

        BEGIN TRANSACTION;
        ALTER TABLE range_2001v1 RENAME TO garbage2;

        /*create a new table with the same column names and types while
        defining a primary key for the desired column*/
        CREATE TABLE range_2001v1 (strHUC12RNG TEXT PRIMARY KEY,
                                intGAPOrigin INTEGER,
                                intGAPPresence INTEGER,
                                intGAPReproduction INTEGER,
                                intGAPSeason INTEGER,
                                strGAPOrigin TEXT,
                                strGAPPresence TEXT,
                                strGAPReproduction TEXT,
                                strGAPSeason TEXT);

        INSERT INTO range_2001v1 SELECT * FROM garbage2;

        DROP TABLE garbage2;
        COMMIT TRANSACTION;

        PRAGMA foreign_keys=on;

        CREATE INDEX idx_range2001_unit ON range_2001v1 (strHUC12RNG);
        """
        cursorQ, conn = spatialite(task_db)
        cursorQ.executescript(sql1)

    else:
        # Create a data frame that represents an empty range, etc.
        # Rename columns and drop some too.
        sql1 = """
        BEGIN TRANSACTION;

        /*create a new table with the same column names and types while
        defining a primary key for the desired column*/
        CREATE TABLE range_2001v1 (strHUC12RNG TEXT PRIMARY KEY,
                                intGAPOrigin INTEGER,
                                intGAPPresence INTEGER,
                                intGAPReproduction INTEGER,
                                intGAPSeason INTEGER,
                                strGAPOrigin TEXT,
                                strGAPPresence TEXT,
                                strGAPReproduction TEXT,
                                strGAPSeason TEXT);

        COMMIT TRANSACTION;

        PRAGMA foreign_keys=on;

        CREATE INDEX idx_range2001_unit ON range_2001v1 (strHUC12RNG);
        """
        cursorQ.executescript(sql1)

    ######################################################## ADD PRESENCE TABLE
    sqll = """
    ATTACH DATABASE '{0}' AS hucs;

    CREATE TABLE presence AS SELECT range_2001v1.strHUC12RNG,
                                    range_2001v1.intGAPPresence AS presence_2001v1,
                                    shucs.geom_5070
                             FROM range_2001v1 LEFT JOIN hucs.huc12rng_gap_polygon as shucs
                                               ON range_2001v1.strHUC12RNG = shucs.HUC12RNG;
    """.format(grid_db)
    try:
        cursorQ.executescript(sqll)
    except Exception as e:
        print(e)

    sql = """
    /* Set a primary key */
    BEGIN TRANSACTION;
    ALTER TABLE presence RENAME TO garbage3;

    /*Create a new table with the same column names and types while
    defining a primary key for the desired column*/
    CREATE TABLE presence (strHUC12RNG TEXT PRIMARY KEY,
                           presence_2001v1 INTEGER,
                           geom_5070);

    INSERT INTO presence SELECT * FROM garbage3;

    DROP TABLE garbage3;
    COMMIT TRANSACTION;
    PRAGMA foreign_keys=on;
    """
    try:
        cursorQ.executescript(sql)
    except Exception as e:
        print(e)

    sql="""
    CREATE INDEX idx_presence_unit ON presence (strHUC12RNG);

    SELECT RecoverGeometryColumn('presence', 'geom_5070', 5070, 'POLYGON',
                                 'XY');
    """
    try:
        cursorQ.executescript(sql)
    except Exception as e:
        print(e)
    conn.commit()


    ########################################################## ADD SUMMER TABLE
    if "S" in seasons:
        sqll = """
        CREATE TABLE summer AS SELECT range_2001v1.strHUC12RNG,
                                        range_2001v1.intGAPSeason AS summer_2001v1,
                                        shucs.geom_5070
                                FROM range_2001v1 LEFT JOIN hucs.huc12rng_gap_polygon as shucs
                                                ON range_2001v1.strHUC12RNG = shucs.HUC12RNG
                                WHERE (range_2001v1.strGAPSeason = 'Summer' 
                                OR range_2001v1.strGAPSeason = 'Year-round')
                                AND (intGAPPresence NOT IN (4, 5));
        """.format(grid_db)
        try:
            cursorQ.executescript(sqll)
        except Exception as e:
            print(e)

        sql = """
        /* Set a primary key */
        BEGIN TRANSACTION;
        ALTER TABLE summer RENAME TO garbage;

        /*Create a new table with the same column names and types while
        defining a primary key for the desired column*/
        CREATE TABLE summer (strHUC12RNG TEXT PRIMARY KEY,
                            summer_2001v1 INTEGER,
                            geom_5070);

        INSERT INTO summer SELECT * FROM garbage;

        DROP TABLE garbage;
        COMMIT TRANSACTION;
        PRAGMA foreign_keys=on;
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)

        sql="""
        CREATE INDEX idx_summer_unit ON summer (strHUC12RNG);

        SELECT RecoverGeometryColumn('summer', 'geom_5070', 5070, 'POLYGON',
                                    'XY');
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)
        conn.commit()

    ########################################################## ADD WINTER TABLE
    if "W" in seasons:
        sqll = """
        CREATE TABLE winter AS SELECT range_2001v1.strHUC12RNG,
                                        range_2001v1.intGAPSeason AS winter_2001v1,
                                        shucs.geom_5070
                                FROM range_2001v1 LEFT JOIN hucs.huc12rng_gap_polygon as shucs
                                                ON range_2001v1.strHUC12RNG = shucs.HUC12RNG
                                WHERE (range_2001v1.strGAPSeason = 'Winter'
                                OR range_2001v1.strGAPSeason = 'Year-round')
                                AND (intGAPPresence NOT IN (4, 5));
        """.format(grid_db)
        try:
            cursorQ.executescript(sqll)
        except Exception as e:
            print(e)

        sql = """
        /* Set a primary key */
        BEGIN TRANSACTION;
        ALTER TABLE winter RENAME TO garbage;

        /*Create a new table with the same column names and types while
        defining a primary key for the desired column*/
        CREATE TABLE winter (strHUC12RNG TEXT PRIMARY KEY,
                            winter_2001v1 INTEGER,
                            geom_5070);

        INSERT INTO winter SELECT * FROM garbage;

        DROP TABLE garbage;
        COMMIT TRANSACTION;
        PRAGMA foreign_keys=on;
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)

        sql="""
        CREATE INDEX idx_winter_unit ON winter (strHUC12RNG);

        SELECT RecoverGeometryColumn('winter', 'geom_5070', 5070, 'POLYGON',
                                    'XY');
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)
        conn.commit()

    ###################################################### ADD YEAR ROUND TABLE
    if "Y" in seasons:
        sqll = """
        CREATE TABLE year_round AS SELECT range_2001v1.strHUC12RNG,
                                        range_2001v1.intGAPSeason AS year_round_2001v1,
                                        shucs.geom_5070
                                FROM range_2001v1 LEFT JOIN hucs.huc12rng_gap_polygon as shucs
                                                ON range_2001v1.strHUC12RNG = shucs.HUC12RNG
                                WHERE range_2001v1.strGAPSeason = 'Year-round'
                                AND intGAPPresence NOT IN (4, 5);
        """.format(grid_db)
        try:
            cursorQ.executescript(sqll)
        except Exception as e:
            print(e)

        sql = """
        /* Set a primary key */
        BEGIN TRANSACTION;
        ALTER TABLE year_round RENAME TO garbage;

        /*Create a new table with the same column names and types while
        defining a primary key for the desired column*/
        CREATE TABLE year_round (strHUC12RNG TEXT PRIMARY KEY,
                            year_round_2001v1 INTEGER,
                            geom_5070);

        INSERT INTO year_round SELECT * FROM garbage;

        DROP TABLE garbage;
        COMMIT TRANSACTION;
        PRAGMA foreign_keys=on;
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)

        sql="""
        CREATE INDEX idx_year_round_unit ON year_round (strHUC12RNG);

        SELECT RecoverGeometryColumn('year_round', 'geom_5070', 5070, 'POLYGON',
                                    'XY');
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)
        conn.commit()

    ######################################################### LAST RECORD TABLE
    if use_observations == True:
        sql="""
        CREATE TABLE last_record (strHUC12RNG TEXT,
                                record_id TEXT,
                                eventDate TEXT,
                                weight INT,
                                proportion_overlap,
                                age_in_weeks INT,
                                date_assessed INT,
                                geom_5070);
        """
        try:
            cursorQ.executescript(sql)
        except Exception as e:
            print(e)

    conn.commit()
    conn.close()
    del cursorQ

    print("Created range database: " + str(datetime.now() - time0))

#  ------------------------------------------------------------ Insert opinions
def insert_opinions(species, seasons, years, task_db):
    """
    Retrieves opinion data from the range_opinions database and filters records
    according to the reconciliation criteria and years of interest.

    PARAMETERS
    ----------
    species : GAP species code
    years : tuple of years of interest
    task_db : path to the range database
    """
    opinion_db = "REPLACETHIS/Vert/DBase/range_opinions.sqlite"
    connection = sqlite3.connect(opinion_db)

    # Define a function for retrieving opinions for a species and season
    def get_opinions(season, species, years):
        """Retrieve opinions for a species and season.

        Parameters
        ----------
        season : str
            Season to retrieve opinions for.
        species : str
            Species code to retrieve opinions for.
        years : list
            List of years to retrieve opinions for.

        Returns
        -------
        pd.DataFrame
            Dataframe of opinions for the specified season, species, and years.
        """
        # Connect to the task database
        connection = sqlite3.connect(opinion_db)

        # Get the opinions for the specified season
        sql = f"""SELECT * FROM {season} WHERE species_code = '{species}';"""
        df = (pd.read_sql(sql, connection)
              [lambda x: x["year"].isin(years) == True])
        
        # Cleanup
        df = cleanup_opinions(df)

        # Add season column
        df["season"] = season

        return df
    
    def cleanup_opinions(df):
        """Local function that handles duplicates, outdated, and negated
        opinions for presence or a season individually.  Assesses and judges
        conflicting opinions within a season or presence."""

        # --------------------------------------------------------- Drop duplicates
        """Records that are identical to another record should be dropped"""
        df2 = df.drop_duplicates()

        # ------------------------------------- Drop older entries from each expert
        """If an expert entered records for the same unit at different times,
        keep the more recent record and drop the other"""
        # Set columns to group on
        df3 = (df2.sort_values(by="entry_time", ascending=False)
              .groupby(['strHUC12RNG', 'year', 'species_code', 'expert'])
              .first()
              .reset_index()
              )
        
        # --------------------------------------------------- Drop negated opinions
        """If experts with equal rank and confidence have opposing opinions, drop all
        records in order to leave them out of range compilation.
        """
        # Add a column for marking records to omit
        df3["omit"] = 0.0

        # Find value combinations of records that need to be dropped/omitted.
        negated = (df3
                .groupby(['strHUC12RNG', 'year', 'expert_rank', 'confidence'])
                .count()
                [lambda x: x["status"] > 1]
                .reset_index()
                )

        # Mark records to omit
        for i in negated.index:
            s_unit = negated.loc[i, "strHUC12RNG"]
            year = negated.loc[i, "year"]
            rank = negated.loc[i, "expert_rank"]
            confidence = negated.loc[i, "confidence"]

            df7 = (df3[(df3["strHUC12RNG"] == s_unit) & (df3["year"] == year) &
                    (df3["expert_rank"] == rank) & (df3["confidence"] == confidence)]
                   .copy())
            df7["omit"] = 1.0
            df3.update(df7)

        # Omit
        df3 = df3[df3["omit"] == 0.0]

        # ------------------------------------ Drop lower expert rank or confidence
        """Multiple experts submit opinions, keep the one with the highest rank
        and drop the others.  If rank is tied, keep higher confidence.
        """
        df5 = (df3
                .drop(["omit"], axis=1)
                .sort_values(by=["expert_rank", "confidence"], ascending=False)
                .groupby(['strHUC12RNG', 'year', 'species_code'])
                .first()
                .reset_index()
                )
        return df5

    def adjust_opinions(seasons, task_db):
        """
        Adjust the opinion values for presence and range based on assessment across
        presence and range.  Range could be summer, winter, or year_round.

        The evaluations follow the logic that season-present implies presence-present 
        and presence-absent implies season-absent, but season-absent does not imply
        presence-absent. Futhermore, year-round present implies present during
        summer and winter, but summer or winter present does not imply year-round
        present.

        Before this code is run, the opinions table has presence and range opinion
        records concatenated vertically and includes records from any season.
        This code adds new columns with adjusted opinions and weights to
        address 4 cases: 
        1) range is "present" but presence is either "absent" or "present" -> ranks are
            assessed to pick a winner.
        2) presence is "absent" and range is NULL -> season becomes "absent"
        3) presence is NULL and range is "present" -> presence becomes "present"
        4) presence is "absent" and range is "absent" -> range absent weight 
            is set to presence absent weight if present absent weight is higher.

        The general steps are to read in the opinions table and expand it as
        necessary by creating dataframes that can be inferred and then
        concatenating them.  For example, range-present records for spatial 
        unit-years without presence records.  Second, we find and assess 
        combintations of spatial unit-year that meet each of the 4 cases listed
        above, then update the opinions table with those assessments.  Some 
        spatial unit-year combinations do not need adjustment, so those are 
        assigned based on existing values for those records.

        Because the range adjustments are based upon presence, presence must
        be adjusted first.  During that process, presence records are created
        for hucs that have range records but no presence records, if the range
        record is "present".
        """
        import sqlite3
        import pandas as pd
        from datetime import datetime
        timestamp = datetime.now()

        # Read in the opinion table
        connection = sqlite3.connect(task_db)
        df = pd.read_sql("SELECT * FROM opinions;", con=connection)

        # Convert seasons
        season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence", "presence": "presence"}
        seasons = [season_dict[x] for x in seasons]
        try:
            seasons.remove("presence")
        except:
            pass

        # EXPAND --------------------------------------------------------------
        try:
            # Create empty objects as placeholders for concat below
            range_to_presence = None
            y_r_to_summer = None
            y_r_to_winter = None

            # Adjust presence first by adding records for hucs that have range 
            # records but no presence records, if the range record is 
            # "present".  
            #Dataframe of range-present records
            present = df[df["status"] == 'present'].copy()

            # Dataframe of presence records (present or absent)
            presence = df[df["season"] == 'presence'].copy()

            # Select records from range-present that are not in presence
            range_to_presence = (present[~present["strHUC12RNG"]
                                        .isin(presence["strHUC12RNG"])]
                                        .copy())
            
            # Change the season of the range-present records to presence
            range_to_presence["season"] = "presence"

            # Create a dataframe of year-round records for both summer and winter
            # Dataframe of year-round records (present or absent)
            year_round = df[df["season"] == 'year_round'].copy()

            # year-round to summer
            y_r_to_summer = year_round.copy()
            y_r_to_summer["season"] = "summer"

            # year-round to winter
            y_r_to_winter = year_round.copy()
            y_r_to_winter["season"] = "winter"

            # Add the new records to the dataframe
            df = pd.concat([df, range_to_presence, y_r_to_summer, 
                            y_r_to_winter])  
        except Exception as e:    
            print(e)

        # ADJUST --------------------------------------------------------------
        # Columns for adjusted values will be filled out, go ahead and add them
        df["status_adjusted"] = pd.NA
        df["weight_adjusted"] = pd.NA

        for season in seasons:
            try:
                # Make new dataframes of all presence records and all range 
                # records
                presence = df[df["season"] == 'presence'].copy()
                range1 = df[df["season"] == season].copy()

                # 1) PRESENCE & RANGE MAY DISAGREE ----------------------------
                # Handle this by updating adjusted status and weight in df 
                #     where appropriate.
                # Dataframe of range-present records
                range_present = range1[range1["status"] == 'present'].copy()

                # Concatenate the dataframes to be able to compare weights of records
                concated1 = pd.concat([range_present, presence.copy()])

                # Find winning records (highest weight) among range-present, presence
                #   records
                winners = (concated1.sort_values(by=["weight"],
                                                 ascending=False)
                           .groupby(['strHUC12RNG', 'year'])
                           .first()
                           .reset_index()
                           .filter(['strHUC12RNG', 'year', 'status', 'weight'])
                           .fillna(pd.NA)
                           )

                # Prep column names for join back with first concatenated data frame
                #   which doesn't include range absent records.
                winners["status_adjusted"] = winners["status"]
                winners["weight_adjusted"] = winners["weight"]

                # Drop columns that are not needed
                winners = winners.drop(['weight', 'status'], axis=1)
                concated1 = concated1.drop(['status_adjusted', 'weight_adjusted'], 
                                           axis=1)

                # Update concated1 with adjusted values from winners
                concated1 = (pd.merge(left=concated1, right=winners,
                                        on=["strHUC12RNG", "year"],
                                        how="left")
                               .filter(['strHUC12RNG', 'year', 'season',
                                      'status_adjusted', 'weight_adjusted'], 
                                     axis=1))

                # Get data from concated1 into df.  Some records will be new
                # to df and others will need to update the adjusted columns.
                df = (pd.merge(left=df, right=concated1,
                               on=["strHUC12RNG", "year", "season"],
                               how="outer", suffixes=(None, "_y")))

                # Set status_adjusted and weight_adjusted to status_adjusted_y and
                # weight_adjusted_y where status_adjusted_y is not null
                df["status_adjusted"] = df["status_adjusted_y"].where(
                    df["status_adjusted_y"].notnull(), df["status_adjusted"])
                df["weight_adjusted"] = df["weight_adjusted_y"].where(
                    df["weight_adjusted_y"].notnull(), df["weight_adjusted"])
                
                # Drop columns that are not needed
                df = df.drop(['status_adjusted_y', 'weight_adjusted_y'], 
                             axis=1)

                # 2) PRESENCE IS ABSENT & NO RANGE RECORD ---------------------
                # Records with presence-absent
                PA = presence[presence["status"] == 'absent'].copy()

                # Presence-absent records without a spatial_unit-year range 
                # record (i.e., no range)
                RNull = (pd.merge(left=PA, right=range1, suffixes=(None, "_y"),
                                  on=["strHUC12RNG", "year"], how="left")
                         .fillna(pd.NA)
                         [lambda x: x["status_y"].isna() == True]
                         .filter(PA.columns, axis=1)
                         )

                # Recode to be records for the range(season)
                RNull["season"] = season
                RNull["status_adjusted"] = RNull["status"]
                RNull["weight_adjusted"] = RNull["weight"]
                RNull["status"] = pd.NA
                RNull["weight"] = pd.NA

                # Add the values in the main dataframe.
                df = pd.concat([df, RNull])

                # 3) RANGE IS PRESENT & NO PRESENCE RECORD --------------------
                # Records with range-present
                RP = range1[range1["status"] == 'present'].copy()

                # range-present records without a spatial_unit-year presence 
                # record (i.e., no presence)
                PNull = (pd.merge(left=RP, right=presence, 
                                  suffixes=(None, "_y"),
                                  on=["strHUC12RNG", "year"], how="left")
                         .fillna(pd.NA)
                         [lambda x: x["status_y"].isna() == True]
                         .filter(RP.columns, axis=1)
                         )
                
                # Recode to be records for presence
                PNull["season"] = 'presence'
                PNull["status_adjusted"] = PNull["status"]
                PNull["weight_adjusted"] = PNull["weight"]
                PNull["status"] = pd.NA
                PNull["weight"] = pd.NA

                # Add the values in the main dataframe.
                df = pd.concat([df, PNull])
                
                # 4) RANGE IS ABSENT & PRESENCE IS ABSENT ---------------------
                # The weight of range-absent records does not imply anything 
                # about the weight of presence-absent status.  However, for
                # spatio-temporal units with "absent" for the range and presence
                # we want to adjust the weight to the higher weight of the two.

                # Records with range-absent
                range_absent = range1[range1["status"] == 'absent'].copy()

                # Records with presence-absent
                presence_absent = presence[presence["status"] == 'absent'].copy()

                # Concatenate the dataframes to be able to compare weights of records
                concated = pd.concat([range_absent, presence_absent])

                # Find winning records (highest weight) among range-absent, 
                # presence-absent records
                winners = (concated.sort_values(by=["weight"], ascending=False)
                            .groupby(['strHUC12RNG', 'year'])
                            .first()
                            .reset_index()
                            .filter(['strHUC12RNG', 'year', 'status', 'weight'])
                            .fillna(pd.NA)
                            )
                
                # Prep column names for join back with concated so that 
                # status_adjusted and weight_adjusted can be updated.
                winners["status_adjusted"] = winners["status"]
                winners["weight_adjusted"] = winners["weight"]
                winners["season"] = season

                # Drop columns that are not needed
                winners = winners.drop(['weight', 'status'], axis=1)
                concated = concated.drop(['status_adjusted', 'weight_adjusted'],
                                            axis=1)
                
                # Update concated with adjusted weight and status from winners.
                concated = (pd.merge(left=concated, right=winners,
                                        on=["strHUC12RNG", "year", "season"],
                                        how="left")
                             .filter(['strHUC12RNG', 'year', 'season', 
                                      'status_adjusted', 'weight_adjusted'], 
                                     axis=1))

                # Get data from concated into df.  No new records will be added.
                # Only the adjusted columns will be updated.
                df = (pd.merge(left=df, right=concated,
                               on=["strHUC12RNG", "year", "season"],
                               how="outer", suffixes=(None, "_y")))

                # Set status_adjusted and weight_adjusted to status_adjusted_y and
                # weight_adjusted_y where status_adjusted_y is not null
                df["status_adjusted"] = df["status_adjusted_y"].where(
                    df["status_adjusted_y"].notnull(), df["status_adjusted"])
                df["weight_adjusted"] = df["weight_adjusted_y"].where(
                    df["weight_adjusted_y"].notnull(), df["weight_adjusted"])
                
                # Drop columns that are not needed
                df = df.drop(['status_adjusted_y', 'weight_adjusted_y'],
                                axis=1)
                
                # WHERE NO ADJUSTMENT IS WARRANTED ----------------------------
                # Fill out the adjusted columns with the original values, where
                # the adjusted columns are null.
                df["status_adjusted"] = df["status_adjusted"].fillna(df["status"])
                df["weight_adjusted"] = df["weight_adjusted"].fillna(df["weight"])

                # WRITE RESULTS -----------------------------------------------
                df.to_sql(name="opinions", con=connection, if_exists="replace")
                connection.commit()
                print(f"Adjusted {season} opinions: {str(datetime.now() - timestamp)}")
            except Exception as e:
                print("!!! Failed to adjust opinions - {0}".format(season))
                print(e)

        #else:
            #print("Presence was not included in seasons - opinions were not adjusted.")

    # ------------------------------------------------------- Retrieve the data
    # Convert years and months value to integer for an SQL statement.
    years = tuple([int(x) for x in years])

    # Create placeholder variables for concat logic below
    dfP2 = None
    dfS2 = None
    dfW2 = None
    dfY2 = None
    
    # Retrieve the opinions for the specified species and years
    try:
        dfP2 = get_opinions("presence", species, years)
        dfS2 = get_opinions("summer", species, years)
        dfW2 = get_opinions("winter", species, years)
        dfY2 = get_opinions("year_round", species, years)

        # Concatenate season and presence opinion tables
        df5 = pd.concat([x for x in [dfP2, dfS2, dfW2, dfY2] if x is not None])

    except Exception as e:
        print(e)

    # Write to opinions table
    try:
        connection = sqlite3.connect(task_db)
        cursor = connection.cursor()
        df5.to_sql("tmp_opinions", connection, if_exists='replace')

        sql = """
        /* Put opinions into a table with a primary key */
        BEGIN TRANSACTION;

        CREATE TABLE opinions (strHUC12RNG TEXT NOT NULL,
                               year INTEGER NOT NULL,
                               species_code TEXT,
                               expert TEXT,
                               status TEXT,
                               expert_rank INTEGER,
                               confidence INTEGER,
                               justification TEXT,
                               entry_time TEXT,
                               season TEXT,
                               type TEXT,
                               PRIMARY KEY (strHUC12RNG, year, season)
                               );

        INSERT INTO opinions
        SELECT strHUC12RNG, year, species_code, expert, status, expert_rank,
            confidence, justification, entry_time, season, type
        FROM tmp_opinions;

        DROP TABLE tmp_opinions;

        CREATE INDEX idx_opinions ON opinions (strHUC12RNG, year, season);

        COMMIT TRANSACTION;

        PRAGMA foreign_keys=on;
        """
        cursor.executescript(sql)
        connection.commit()
    except Exception as e:
        print(e)

    # Add a weight column
    try:
        connection = sqlite3.connect(task_db)
        cursor = connection.cursor()
        sql = """
        /* Add weight column */
        ALTER TABLE opinions ADD COLUMN weight REAL;

        UPDATE opinions
        SET weight = expert_rank * confidence/10.0;
        """
        cursor.executescript(sql)
        connection.commit()
    except Exception as e:
        print(e)

    # Adjust status and weights 
    try:
        adjust_opinions(seasons, task_db)
    except Exception as e:
        print(e)

def make_references_table(species, task_db):
    """
    Makes a reference table with references from opinions/GAP and occurrence
    records databases.
    
    PARAMETERS
    ----------
    species : GAP species code
    years : tuple of years of interest
    task_db : path to the range database
    """
    # Create a table for references in task_db with columns GAP_code and 
    #   reference_text.
    import sqlite3
    import pandas as pd
    import datetime

    time0 = datetime.datetime.now()

    # Connect to the task database
    connection_task = sqlite3.connect(task_db)

    # ----------------------------------------------- Add literature references
    if use_opinions:
        # Connect to the opinions database
        opinion_db = "REPLACETHIS/Vert/DBase/range_opinions.sqlite"
        connection_op = sqlite3.connect(opinion_db)

        try:
            # Connect to the task database
            connection = sqlite3.connect(opinion_db)

            # Build a set of reference codes from all opinion tables
            reference_codes = []
            for season in ['presence', 'summer', 'winter', 'year_round']:
                sql = f"""
                SELECT citations FROM {season} WHERE species_code = '{species}';
                """
                df = pd.read_sql(sql, con=connection)

                # Prep citation code list for an SQL statement
                citation_list = df['citations'].unique()
                if len(citation_list) > 0:
                    for x in citation_list:
                        y = x.split(',')
                        for z in y:
                            reference_codes.append(z.strip())
            connection.close()
            
            # From the GAP database, get the reference text for the reference codes
            GAPcursor, GAPconnection = database.ConnectDB('GapVert_48_2016')

            # Build a string of reference codes for the SQL statement, each item
            # needs to be a string and the whole thing needs to be in parentheses
            if len(reference_codes) > 1:
                reference_codes = str(tuple(reference_codes))
            else:
                reference_codes = str(tuple(reference_codes)).replace(",", "")
            sql = f"""
            SELECT strRefCode, memCitation FROM dbo.tblCitations
            WHERE strRefCode IN {reference_codes}; 
            """

            df = pd.read_sql(sql, con=GAPconnection)
            df.rename(columns={'strRefCode': 'GAP_code', 
                            'memCitation': 'reference_text'}, inplace=True)
            GAPconnection.close()
            
            # Write the references df to the references table.
            df.to_sql("references", connection_task, if_exists='append',
                      index=False)
            connection.close()

        except Exception as e:
            print("Failed to add opinion references.")
            print(e)
    
    # ------------------------------------------------ Add observation datasets
    if use_observations:
        try:
            # Connect to the task database
            connection_task = sqlite3.connect(task_db)

            for db in ww_output:
                # Connect to an occurrence records database
                connection_ww = sqlite3.connect(db)

                # Get string of citation string from GBIF_download_info table
                sql = """
                SELECT citations FROM GBIF_download_info;
                """
                cursor = connection_ww.cursor()
                cursor.execute(sql)
                citation_string = cursor.fetchone()[0]
                connection_ww.close()

                # Parse out the individual citations. Citations start after "rights.txt:" and are each on a separate line.
                citations = citation_string.split("rights.txt:")[1].split("\n")
                citations = [x.strip() for x in citations if x]

                # Make a dataframe of the citations with GAP_code and reference_text columns, GAP_code will be the db name for each record.
                df = pd.DataFrame(citations, columns=['reference_text'])
                df['GAP_code'] = db.split("/")[-1].replace(".sqlite", "")
                
                # Write the references df to the references table.
                df.to_sql("references", connection_task, if_exists='append',
                           index=False)
            
            connection_task.close()

        except Exception as e:
            print("Failed to add observation references.")
            print(e)
        
    print("Created references table: " + str(datetime.datetime.now() - time0))

#  -------------------------------------------------- Insert occurrence records
def insert_records(years, months, task_name, workDir, task_db, codeDir):
    '''
    Gets records from the occurrence record shapefile and add them to the
    range db.  Also, filters out records from unwanted years and months.
    '''
    from datetime import datetime
    import os

    # Connect to the task database
    try:
        cursor, conn = spatialite(task_db)
    except Exception as e:
        print(e)

    # Get records from the shapefiles, starting with the first db
    db = ww_output[0]
    shp_name = db.split("/")[-1].replace(".sqlite", "")
    shp_path = workDir + "/" + shp_name + "/" + shp_name
    try:
        timestamp = datetime.now()
        cursor.execute("""SELECT ImportSHP(?, 'occurrence_records',
                          'UTF-8', 5070, 'geometry', 'record_id', 'POLYGON');""",
                          (shp_path,))
        conn.commit()
        print("Imported one occurrence records shapefile: ",
              str(datetime.now() - timestamp))
    except Exception as e:
        print(e)

    # Insert additional databases
    if len(ww_output) > 1:
        print(1)
        for db in ww_output[1:]:
            print(2)
            shp_name = db.split("/")[-1].replace(".sqlite", "")
            shp_path = workDir + "/" + shp_name + "/" + shp_name
            print(shp_path)
            print(shp_name)
            try:
                print(3)
                timestamp = datetime.now()
                cursor.executescript("""SELECT ImportSHP('{0}', '{1}', 'UTF-8',
                                     5070, 'geometry', 'record_id', 'POLYGON');

                                     INSERT INTO occurrence_records
                                        SELECT * FROM {1}
                                        WHERE record_id
                                        NOT IN (SELECT record_id FROM occurrence_records);

                                     DROP TABLE {1};
                                     """.format(shp_path, shp_name))
                conn.commit()
                print("Imported another occurrence records shapefile: ", str(datetime.now() - timestamp))
            except Exception as e:
                print(e)

    # Create indexes
    try:
        timestamp = datetime.now()
        sql = """
        CREATE INDEX idx_eo_date ON occurrence_records (eventDate);
        CREATE INDEX idx_eo_id ON occurrence_records (record_id);
        """
        cursor.executescript(sql)
        conn.commit()
        print("Indexed the occurrence records: ", str(datetime.now() - timestamp))
    except Exception as e:
        print(e)

    # Drop records with unwanted years and months
    try:
        timestamp = datetime.now()
        # Convert years and months value to integer for an SQL statement.
        years = tuple([int(x) for x in years])
        months = tuple([int(x) for x in months])
        sql="""DELETE FROM occurrence_records
               WHERE CAST(STRFTIME('%Y', eventDate) AS INTEGER) NOT IN {0};

               DELETE FROM occurrence_records
               WHERE CAST(STRFTIME('%m', eventDate) AS INTEGER) NOT IN {1}""".format(years, months)
        cursor.executescript(sql)
        conn.commit()
        print("Dropped records with unwanted dates: ", str(datetime.now() - timestamp))
    except Exception as e:
        print(e)

    # Register the geometry column
    try:
        timestamp = datetime.now()
        cursor.execute("""SELECT RecoverGeometryColumn('occurrence_records', 'geometry',
                          5070, 'POLYGON', 'XY');""")
        conn.commit()
        print("Registered geometry column: ", str(datetime.now() - timestamp))
    except Exception as e:
        print(e)

    # Close db
    conn.close()

#  ----------------------------------------------- Get records for a time frame
def get_records(start_year, end_year, conn, cursor, era, season):
    """
    Get the appropriate species occurrence records to use for a time frame.

    PARAMETERS
    ----------
    start_year : integer
    end_year : integer
    cursor : sqlite3 cursor
    season : string like "summer", "winter" or "year_round"
    era : string
        'recent' or 'historical'
    """
    from datetime import datetime
    time1 = datetime.now()

    # Build an era condition
    if era == 'recent':
        condition = '> ' + str(start_year) + " AND eventDate < " + str(int(end_year) + 1)
    else:
        condition = '< ' + str(start_year)
    
    # Build a season condition                                                 # Develop spatial-unit specific season dates somehere around here.
    condition2 = ''
    if season == "summer":
        condition2 = "AND month in ('05', '06', '07')"
    if season == "winter":
        condition2 = "AND month in ('12', '01', '02')"        

    # Get the records ---------------------------------------------------------
    sql="""
    CREATE TABLE {0}_records (taxon_id TEXT,
                              record_id PRIMARY KEY,
                              eventDate TEXT,
                              weight TEXT,
                              weight_notes TEXT,
                              month TEXT,
                              geometry);

    INSERT INTO {0}_records SELECT taxon_id, record_id, eventDate, weight,
                                   weight_not AS weight_notes,
                                   STRFTIME('%m', eventDate) AS month,
                                   geometry
                            FROM occurrence_records
                            WHERE eventDate {1} {2};
    """.format(era, condition, condition2)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Checking spatial metadata for get_records TEMPORARYSTATEMENT TEMPORARYSTATEMENT")
        print(cursor.execute('SELECT checkSpatialMetaData();').fetchall())
        print("Created a table of {0}-{1} records: ".format(end_year, era) + str(datetime.now()-time1))
    except Exception as e:
        print("!!! FAILED to create a table of {0}-{1} records: ".format(end_year, era) + str(datetime.now()-time1))
        print(e)

    sql = """
    CREATE INDEX idx_{0}s ON {0}_records (eventDate);

    SELECT RecoverGeometryColumn('{0}_records', 'geometry', 5070, 'POLYGON', 
                                 'XY');

    SELECT CreateSpatialIndex('{0}_records', 'geometry');
    """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Created indexes for {0}-{1} records: ".format(end_year, era) + str(datetime.now()-time1))
    except Exception as e:
        print("!!! FAILED to create indexes for {0}-{1} records: ".format(end_year, era) + str(datetime.now()-time1))
        print(e)

# -------------------------------------------------- Intersect records and grid
def intersect(era, end_year, conn, cursor):
    """
    Intersects occurrence records and the grid

    PARAMETERS
    ----------
    era : string
        'recent' or 'historical'
    end_year: integer
        used for print statement only
    """
    from datetime import datetime
    time1 = datetime.now()

    sql="""
    CREATE TABLE intersected_{0} (HUC12RNG TEXT,
                                  record_id TEXT,
                                  eventDate TEXT,
                                  weight TEXT,
                                  geom_5070);
        """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
    except Exception as e:
        print("!! FAILED to create intersected_{0}-{1} table: ".format(era, end_year) + str(datetime.now()-time1))
        print(e)

    sql="""
    INSERT INTO intersected_{0} SELECT hp.HUC12RNG AS HUC12RNG,
                                   eo.record_id AS record_id,
                                   eo.eventDate AS eventDate,
                                   eo.weight AS weight,
                                   CastToMultiPolygon(Intersection(hp.geom_5070,
                                                      eo.geometry)) AS geom_5070
                                FROM huc12rng_gap_polygon as hp, {0}_records AS eo
                                WHERE ST_Intersects(hp.geom_5070, eo.geometry) = 1
                                AND hp.ROWID IN (
                                       SELECT ROWID
                                       FROM SpatialIndex
                                       WHERE f_table_name = 'DB=shucs.huc12rng_gap_polygon'
                                       AND f_geometry_column = 'geom_5070'
                                       AND search_frame = eo.geometry
                                                 );
        """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Found and inserted subregions that intersect a {0}-{1} occurrence: ".format(end_year, era) + str(datetime.now()-time1))
    except Exception as e:
        print("!! FAILED to find hucs that intersect a {0}-{1} occurrence: ".format(end_year, era) + str(datetime.now()-time1))
        print(e)

    sql="""
    /* Choice of 'MULTIPOLYGON' here is important. */
    SELECT RecoverGeometryColumn('intersected_{0}', 'geom_5070', 5070,
                                 'MULTIPOLYGON', 'XY');

    CREATE INDEX idx_intersect_{0}s ON intersected_{0} (HUC12RNG, record_id, eventDate, weight);
    """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Created geometry column and index for {0}-{1} subregion intersections: ".format(end_year, era) + str(datetime.now()-time1))
    except Exception as e:
        print("!! FAILED to create geometry and/or index for {0}-{1}: ".format(end_year, era) + str(datetime.now()-time1))
        print(e)

# -------------------------------- Filter out small fragments from intersection
def filter_small(era, end_year, task_id, gap_id, conn, cursor):
    """
    Use the error tolerance for the species to select those occurrences that
    can be attributed to a HUC.

    PARAMETERS
    ----------
    task_id : string
        The name of the range database
    gap_id : string
        The GAP code of the species
    era : string
        "historical" or "recent"

    OUTPUT
    ------
    big_nuff_[recent or historical] : table
        Records from table intersected_recent that have enough overlap to
        attribute to a huc.
    """
    import sqlite3
    from datetime import datetime
    time1 = datetime.now()
    sql="""
    CREATE TABLE big_nuff_{2} (HUC12RNG TEXT,
                               record_id TEXT,
                               eventDate TEXT,
                               weight INTEGER,
                               proportion_circle,
                               geom_5070);

    INSERT INTO big_nuff_{2} SELECT intersected_{2}.HUC12RNG,
                                    intersected_{2}.record_id,
                                    intersected_{2}.eventDate,
                                    intersected_{2}.weight,
                                    100 * (ST_Area(intersected_{2}.geom_5070) / ST_Area(eo.geometry))
                                        AS proportion_circle,
                                    intersected_{2}.geom_5070
                             FROM intersected_{2}
                                  LEFT JOIN occurrence_records AS eo
                                  ON intersected_{2}.record_id = eo.record_id
                             WHERE proportion_circle BETWEEN (100 - (SELECT error_tolerance
                                                                     FROM params.tasks
                                                                     WHERE task_id = '{0}'
                                                                     AND species_id = '{1}'))
                                                     AND 100
                             ORDER BY proportion_circle ASC;

      CREATE INDEX idx_bn_{2} ON big_nuff_{2} (HUC12RNG, record_id);
    """.format(task_id, gap_id, era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined which records overlap enough ({0}-{1}): '.format(end_year, era) + str(datetime.now() - time1))
    except Exception as e:
        print(e)
        ("!!!!",era, end_year)

# ------------------------------------------------ Calculate weight of evidence
def calculate_weight(season, era, end_year, conn, cursor):
    """
    Column to make note of hucs in presence that have enough evidence.

    PARAMETERS
    ----------
    era : string
        'recent' or 'historical'
    end_year : integer
    """
    import sqlite3
    from datetime import datetime

    time1 = datetime.now()
    sql="""
    ALTER TABLE {2} ADD COLUMN {1}_weight_{0} INT;

    UPDATE {2}
    SET {1}_weight_{0} = (SELECT SUM(weight)
                          FROM big_nuff_{1}
                          WHERE HUC12RNG = {2}.strHUC12RNG
                          GROUP BY HUC12RNG);
        """.format(str(end_year), era, season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Calculated total weight of evidence for each subregion ({0}-{1}): '.format(end_year, era) + str(datetime.now() - time1))
    except Exception as e:
        print(e)
        print("!!!!!!", era, end_year, season)

# ---------------------------------------------- Find newly occupied subregions
def new_subregions(season, era, end_year, conn, cursor):
    """
    Find hucs that contained occurrences, but were not in GAP range and
    insert them into the presence table as new records.

    PARAMETERS
    ----------
    era : string
        'recent' : 'historical'
    end_year : integer
    """
    from datetime import datetime

    time1 = datetime.now()
    sql="""
    INSERT INTO {2} (strHUC12RNG)
                SELECT DISTINCT big_nuff_{1}.HUC12RNG
                FROM big_nuff_{1} LEFT JOIN {2}
                                  ON {2}.strHUC12RNG = big_nuff_{1}.HUC12RNG
                WHERE {2}.strHUC12RNG IS NULL;
    """.format(str(end_year), era, season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Added rows for hucs with enough weight but not in GAP range ({0}-{1}-{2}): '.format(end_year, era, season) + str(datetime.now() - time1))
    except Exception as e:
        print(e)
        print("!!!!!",era, end_year, season)

# ----------------------------------------------- Add documented present column
def set_documented(season, era, conn, cursor, end_year, start_year,
                   use_observations):
    """
    Mark records/subregions that have sufficient evidence of presence

    PARAMETERS
    ----------
    era : string
        'recent' : 'historical'
    end_year : integer
    """
    import sqlite3
    from datetime import datetime

    time1 = datetime.now()
    if era == 'recent':
        if use_observations:
            sql="""
                ALTER TABLE {1} ADD COLUMN documented_{0} INT;

                UPDATE {1} SET documented_{0} = 1 WHERE recent_weight_{0} >= 10;
                """.format(str(end_year), season)
        if not use_observations:
            sql="""
                ALTER TABLE {1} ADD COLUMN documented_{0} INT;
                """.format(str(end_year), season)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out documented recent column ({0}): '.format(end_year) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!!!", era, end_year)

    if era == 'historical':
        if use_observations:
            sql="""
                ALTER TABLE {2} ADD COLUMN documented_pre{1} INT;

                UPDATE {2} SET documented_pre{1} = 1 WHERE historical_weight_{0} >= 10;
                """.format(str(end_year), str(start_year), season)
        if not use_observations:
            sql=""" ALTER TABLE {2} ADD COLUMN documented_pre{1} INT;
                """.format(str(end_year), str(start_year), season)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out documented historical column ({0}-{1}): '.format(end_year, era) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!!!", era, end_year)

'''
# ----------------------------------------------------- Fill out presence codes DELET THIS???????????
def presence_code(period, periods, conn, cursor, version):
    """
    Fills out values in the presence column

    PARAMETERS
    ----------
    period : tuple
        The time period
    version : string
        Range version being created
    """
    from datetime import datetime
    #start_year = str(period[0])
    end_year = str(period[1])

    # Identify previous presence column
    if periods.index(period) > 0:
        previous_season = str(periods[periods.index(period) - 1][1]) + version
    else:
        previous_season = "2001v1"

    # Fill out new presence column
    time1 = datetime.now()

    # ----------------------  PASS 1  --------------------------
    # NOTE: The order of these statements matters and reflects their rank
    if period == periods[0]:
        sql = """
        /* Add columns */
        ALTER TABLE presence ADD COLUMN presence_{0} INT;

        /* --------------------------- 2001v1 -----------------------------------*/
        /* If a 2001v1 code exists, use that as a start */
        UPDATE presence SET presence_{0} = presence_2001v1;

        /* Old legend values 1,2,3 become new legend value 3 */
        UPDATE presence SET presence_{0} = 3 WHERE presence_{0} in (1,2,3);

        /* Old legend values 4,5 become new legend value 4 */
        UPDATE presence SET presence_{0} = 4 WHERE presence_{0} in (4,5);
        """.format(str(end_year) + version)
    
    else:
        sql = """
        /* Add columns */
        ALTER TABLE presence ADD COLUMN presence_{0} INT;

        /* -------------------- Previous Period Code ----------------------------*/
        /* If coded as documented present in previous time step, code as 3 */
        UPDATE presence SET presence_{0} = 3 WHERE presence_{1} = 1;

        /* If coded as likely present in previous time step, code as 2 */
        UPDATE presence SET presence_{0} = 2 WHERE presence_{1} = 2;

        /* If coded as suspected present in previous time step, code as 3 */
        UPDATE presence SET presence_{0} = 3 WHERE presence_{1} = 3;

        /* If coded as suspected absent in previous time step, code as 4 */
        UPDATE presence SET presence_{0} = 4 WHERE presence_{1} = 4;

        /* If coded as likely absent in previous time step, code as 5 */
        UPDATE presence SET presence_{0} = 5 WHERE presence_{1} = 5;
        """.format(str(end_year) + version, previous_season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Added presence column for {0}: '.format(str(end_year) + version) + str(datetime.now() - time1))
    except Exception as e:
        print(e)

    sql="""   
    /* --------------------------- Opinion ----------------------------------*/
    /* If opinion with any score exists, but all else in null base the presence
       on it.*/
    /*Suspected present*/
    UPDATE presence SET presence_{0} = 3 WHERE opinion_{0} = 1
                                         AND presence_2001v1 IS NULL
                                         AND presence_{1} IS NULL
                                         AND documented_{0} IS NULL;

    /*Suspected absent*/
    UPDATE presence SET presence_{0} = 4 WHERE opinion_{0} = 0
                                         AND presence_2001v1 IS NULL
                                         AND presence_{1} IS NULL
                                         AND documented_{0} IS NULL;

    /* If opinion with a high enough score exists, use it to overwrite null
       values and codes from previous periods (including 2001v1)*/
    /*Suspected present*/
    UPDATE presence 
    SET presence_{0} = 3 WHERE opinion_{0} = 1 AND opinion_{0}_weight > 2.0;

    /*Suspected absent*/
    UPDATE presence 
    SET presence_{0} = 4 WHERE opinion_{0} = 0 AND opinion_{0}_weight > 2.0;

    /*Likely present*/
    UPDATE presence 
    SET presence_{0} = 2 WHERE opinion_{0} = 1 AND opinion_{0}_weight > 8.0;

    /*Likely absent*/
    UPDATE presence 
    SET presence_{0} = 5 WHERE opinion_{0} = 0 AND opinion_{0}_weight > 8.0;

    /* ----------------------- Occurrence Records ---------------------------*/
    /* If documented in a previous time period, code as 3 */
    UPDATE presence SET presence_{0} = 1 WHERE documented_{0}=1;

    """.format(str(end_year) + version, previous_season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined {0} range presence value : '.format(str(end_year) + version) + str(datetime.now() - time1))
    except Exception as e:
        print(e)
'''
# ------------------------------------------------------- Fill out season codes
def assign_code(season, period, periods, conn, cursor):
    """
    Fills out values in the presence or season column

    PARAMETERS
    ----------
    period : tuple
        The time period
    """
    from datetime import datetime
    start_year = str(period[0])
    end_year = str(period[1])

    season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence", "presence": "presence"}
    season = season_dict[season]

    # Identify previous season column
    if periods.index(period) > 0:
        previous_season = str(periods[periods.index(period) - 1][1])
    else:
        previous_season = "2001v1"

    # Fill out new presence column
    time1 = datetime.now()

    # ----------------------  PASS 1  --------------------------
    # NOTE: The order of these statements matters and reflects their rank
    if period == periods[0]:
        if season == "presence":
            sql = """
            /* Add columns */
            ALTER TABLE presence ADD COLUMN presence_{0} INT;

            /* --------------------------- 2001v1 ---------------------------*/
            /* If a 2001v1 code exists, use that as a start */
            UPDATE presence SET presence_{0} = presence_2001v1;

            /* Old legend values 1,2,3 become new legend value 3 */
            UPDATE presence SET presence_{0} = 3 WHERE presence_{0} in (1,2,3);

            /* Old legend values 4,5 become new legend value 4 */
            UPDATE presence SET presence_{0} = 4 WHERE presence_{0} in (4,5);
            """.format(str(end_year))
        
        # Year-round  
        if season == "year_round":
            sql = """
            /* Add column */
            ALTER TABLE year_round ADD COLUMN year_round_{0} INT;

            /* If a 2001v1 code exists, use that as a start */
            UPDATE year_round SET year_round_{0} = year_round_2001v1;

            /* ----------------------- 2001v1 ---------------------------*/
            /* Old legend value 1 become new legend value 3 */
            UPDATE year_round SET year_round_{0} = 3 
                WHERE year_round_2001v1 = 1;
            """.format(str(end_year))
        
        # Summer  
        if season == "summer":
            sql = """
            /* Add column */
            ALTER TABLE summer ADD COLUMN summer_{0} INT;

            /* If a 2001v1 code exists, use that as a start */
            UPDATE summer SET summer_{0} = summer_2001v1;

            /* ----------------------- 2001v1 ---------------------------*/
            /* Old legend value 1 or 4 become new legend value 3 */
            UPDATE summer SET summer_{0} = 3 
                WHERE summer_2001v1 = 1 
                OR summer_2001v1 = 4;
            """.format(str(end_year))

        # Winter  
        if season == "winter":
            sql = """
            /* Add column */
            ALTER TABLE winter ADD COLUMN winter_{0} INT;

            /* If a 2001v1 code exists, use that as a start */
            UPDATE winter SET winter_{0} = winter_2001v1;

            /* ----------------------- 2001v1 ---------------------------*/
            /* Old legend value 3 or 1 become new legend value 3 */
            UPDATE winter SET winter_{0} = 3 
                WHERE winter_2001v1 = 1 
                OR winter_2001v1 = 3;
            """.format(str(end_year))

        try:
            cursor.executescript(sql)
            conn.commit()
            print('Added {1} column for {0}: '.format(str(end_year), season) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
             
    if period != periods[0]:
        sql = """
        /* Add columns */
        ALTER TABLE {2} ADD COLUMN {2}_{0} INT;

        /* -------------------- Previous Period Code ------------------------*/
        /* If coded as documented in previous time step, code as 3 */
        UPDATE {2} SET {2}_{0} = 3 WHERE {2}_{1} = 1;

        /* If coded as likely present or range in previous time step, code as 2 */
        UPDATE {2} SET {2}_{0} = 2 WHERE {2}_{1} = 2;

        /* If coded as suspected present or range in previous time step, code as 3 */
        UPDATE {2} SET {2}_{0} = 3 WHERE {2}_{1} = 3;

        /* If coded as suspected absent or non-range in previous time step, code as 4 */
        UPDATE {2} SET {2}_{0} = 4 WHERE {2}_{1} = 4;

        /* If coded as likely absent or non-range in previous time step, code as 5 */
        UPDATE {2} SET {2}_{0} = 5 WHERE {2}_{1} = 5;
        """.format(str(end_year), previous_season, season)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Added {1} column for {0}: '.format(str(end_year), season) + str(datetime.now() - time1))
        except Exception as e:
            print(e)

    sql="""   
    /* --------------------------- Opinion ----------------------------------*/
    /* If opinion with any score exists, but all else in null base the presence
       on it.*/
    /*Suspected present*/
    UPDATE {2} SET {2}_{0} = 3 WHERE opinion_{0} = 1
                                         AND {2}_2001v1 IS NULL
                                         AND {2}_{1} IS NULL
                                         AND documented_{0} IS NULL;

    /*Suspected absent*/
    UPDATE {2} SET {2}_{0} = 4 WHERE opinion_{0} = 0
                                         AND {2}_2001v1 IS NULL
                                         AND {2}_{1} IS NULL
                                         AND documented_{0} IS NULL;

    /* If opinion with a high enough score exists, use it to overwrite null
       values and codes from previous periods (including 2001v1)*/
    /*Suspected present*/
    UPDATE {2}
    SET {2}_{0} = 3 WHERE opinion_{0} = 1 AND opinion_{0}_weight > 2.0;

    /*Suspected absent*/
    UPDATE {2}
    SET {2}_{0} = 4 WHERE opinion_{0} = 0 AND opinion_{0}_weight > 2.0;

    /*Likely present*/
    UPDATE {2}
    SET {2}_{0} = 2 WHERE opinion_{0} = 1 AND opinion_{0}_weight > 8.0;

    /*Likely absent*/
    UPDATE {2}
    SET {2}_{0} = 5 WHERE opinion_{0} = 0 AND opinion_{0}_weight > 8.0;

    /* ----------------------- Occurrence Records ---------------------------*/
    /* If documented, code as 1 */
    UPDATE {2} SET {2}_{0} = 1 WHERE documented_{0}=1;

    """.format(str(end_year), previous_season, season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined {0} range value : '.format(str(end_year)) + str(datetime.now() - time1))
    except Exception as e:
        print(e)

# ---------------------------------------------------------- Flag extralimitals
def flag_extralimitals(season, period, conn, cursor, limit_distance=40000):
    """
    Finds and flags spatial units with documented presence due to occurrence
    records of extralimital individuals.

    Identifies spatial units with documented presence that likely came from 
    extralimital individuals, such as vagrants.  Identifies such units in a 
    column. Extralimitals are defined by only being documented in one time 
    period AND being located more than a set distance from another unit that 
    is coded within the time period being assessed.  This assessment is done on
    a per-period basis because range limits can change over time periods.

    The general strategy for this task is to create 3 geodataframes.  One is 
    all the records with a present code for the time period.  The others are 
    subsets of that one. gdf2 is only records from the first that also don't 
    have documented in any other presence-time period column.  gdf3 is records
    from the first geodataframe that aren't in the gdf2.  The extralimitals are
    a subset of gdf2 that also don't have any "nearby" neighbors. Potential 
    neighbors are units from gdf3, so nearest neighbors from gdf3 for each 
    record in gdf2 is computed with a cKDTree.  Polygons are treated as points 
    for this so it is all approximate. 

    PARAMETERS
    ----------
    period : tuple
        The time period
    conn : conn 
        Sqlite connections with spatialite enabled
    cursor : cursor object
    limit_distance : integer
        Maximum distance (m) that a unit can be from another non-documented
        presence unit before it gets classified as extralimital.
    """
    import geopandas as gpd
    import numpy as np
    from scipy.spatial import cKDTree
    import pandas as pd
    from datetime import datetime
    
    time1 = datetime.now()
    year = str(period[1])

    season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence", "presence": "presence"}
    season = season_dict[season]

    try:
        # Geodataframe of present (values 1,2,or3) in the time period ---------
        # Use the centroids of polygons
        sql = """SELECT strHUC12RNG, 
                        ST_AsBinary(ST_Centroid(geom_5070)) AS geometry  
                FROM {1}
                WHERE {1}_{0} in (1,2,3);
                """.format(year, season)
        gdf1 = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col='geometry', 
                                             crs=5070, index_col='strHUC12RNG')
        conn.commit()

        # Geodataframe of hucs with only one period as documented -------------
        dfD = pd.read_sql("""SELECT * 
                            FROM {1}
                            WHERE {1}_{0} = 1;""".format(year, season),
                            conn)
        
        # Drop excess columns 
        doc_cols = [x for x in list(dfD.columns) if "documented_2" in x] + ["strHUC12RNG"]
        dfD2 = dfD[doc_cols].set_index("strHUC12RNG")
        
        # Select rows with only one documented period
        dfD3 = dfD2.sum(axis=1)
        df_doc1 = pd.DataFrame(dfD3[(dfD3 < 2) & (dfD3 > 0)])
        
        # Merge back with first geodatabase to make a geodataframe
        gdf2 = gdf1.merge(df_doc1, how='inner', left_index=True, 
                          right_index=True).drop([0], axis=1)
        
        # Presence geodataframe, but without single-period documented records -----
        gdf3 = gdf1[~gdf1.index.isin(list(gdf2.index))]
        
        # Calculate distance between documented and nearest presence neighbor -----
        # Rename for ease of interpretation, and set index
        gdA = gdf2.reset_index() # potential vagrant
        gdB = gdf3.reset_index() # all presence

        # Point geometries as arrays
        nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))),
                      dtype="float64")
        nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))),
                      dtype="float64")
        
        # Build a cKDTree for fast lookup/indexing
        if len(nB) > 1 and len(nA) > 1:
            btree = cKDTree(nB)
            
            # Make geodataframe with nearest neighbor distances for gdf2 
            dist, idx = btree.query(nA, k=1)
            
            gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
            gdB_nearest.rename({"strHUC12RNG": "nearest_neighbor"}, 
                                axis=1, inplace=True)
            gdf = pd.concat(
                [
                    gdA.reset_index(drop=True),
                    gdB_nearest,
                    pd.Series(dist, name='dist')
                ],
                axis=1)
            
            # Select records with distance above the cutoff
            outdf = gdf[gdf["dist"] > limit_distance]
            change_units = str(tuple(outdf['strHUC12RNG']))
        
        else:
            outdf = pd.DataFrame()

        # Set extralimital column values ------------------------------------------
        # Add columns
        try:
            sql = """ALTER TABLE {1} ADD COLUMN extralimital_{0} INT;
                  """.format(year, season)
            cursor.execute(sql)
        except Exception as e:
            print(e)
        
        if len(outdf) == 1:
            sql = """UPDATE {2}
                     SET extralimital_{0} = 1
                     WHERE strHUC12RNG = '{1}';
                     """.format(year, outdf.iloc[0]['strHUC12RNG'], season)
            cursor.executescript(sql)
        
        elif len(outdf) > 1:
            cursor.executescript("""                          
                            UPDATE {2}
                            SET extralimital_{0} = 1
                            WHERE strHUC12RNG IN {1};""".format(year,
                                                                change_units,
                                                                season))
        
        else:
            pass

        conn.commit()

        print('Flagged {0} extralimitals : '.format(year)  + str(datetime.now() - time1))

    except Exception as e:
        print(e)

# ------------------------------------------------------------ Code adjustments
def adjust_code(season, periods, period, conn, cursor):
    """
    Adjusts the code of spatial units according to logical rules.  
    Refines/corrects codes assigned by assign_code().

    - Value is changed to suspected absent for extralimital spatial units
        in the period after documented present.

    PARAMETERS
    ----------
    period : tuple
        The time period
    conn : conn 
        Sqlite connections with spatialite enabled
    cursor : cursor object
    """
    from datetime import datetime
    time1 = datetime.now()

    season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence", "presence": "presence"}
    season = season_dict[season]

    # Determine previous and subsequent years ---------------------------------
    year = period[1]
    if period != periods[0]:
        previous_year = periods[periods.index(period) - 1][1]
    else:
        previous_year = None
    
    if period != periods[-1]:
        subsequent_year = periods[periods.index(period) + 1][1]
    else:
        subsequent_year = None

    # Cases where code should be set to suspected present ---------------------
    # Surrounded by documented periods -> set to likely present
    if period != periods[0] and period != periods[-1]:
        try:
            sql="""
            UPDATE {3} SET {3}_{0} = 2
            WHERE {3}_{1} = 1
            AND {3}_{2} = 1
            AND opinion_{0}_weight > 2.0;
            """.format(year, previous_year, subsequent_year, season)
            cursor.executescript(sql)
            conn.commit()
        except Exception as e:
            print(e)

    # Cases where code should be set to suspected absent ----------------------
    # Is flagged extralimital & was documented last period -> suspected absent
    try:
        # Build text for and condition with all period-extralimital columns
        and_clause = "(extralimital_{0} = 1".format(str(periods[0][1]))
        for period in periods[1:]:
            year_ = period[1]
            and_clause = and_clause + " OR extralimital_{0} = 1".format(year_)
        and_clause = and_clause + ")"

        sql="""
        UPDATE {2} SET {2}_{0} = 4
        WHERE {2}_{0} != 1
        AND opinion_{0}_weight IS NULL
        AND {1};
        """.format(year, and_clause, season)

        cursor.executescript(sql)
        conn.commit()
    except Exception as e:
        print(e)

    # Extralimitals and range -------------------------------------------------
    # Is flagged extralimital -> suspected absent if not the presence map/season
    if season != "presence":
        try:
            sql=f"""
            UPDATE {season} SET {season}_{year} = 4
            WHERE {season}_{year} = 1
            AND extralimital_{year} = 1;
            """
            cursor.executescript(sql)
            conn.commit()
            print('Adjusted {0} range value : '.format(year) + str(datetime.now() - time1))
        except Exception as e:
            print(e)

# -------------------------------------------------------- Years since a record
def last_record(task_id, gap_id, task_db, parameters_db, workDir, codeDir,
                grid_db, lock):
    """
    Calculate weeks since a record for each spatial unit, as well as the
        weight of the last record.
    """
    import sqlite3
    from datetime import datetime
    time0 = datetime.now()

    cursor, conn = spatialite()

    cursor.executescript("""/*Attach databases*/
                            ATTACH DATABASE '{0}' AS params;
                            ATTACH DATABASE '{1}' AS shucs;
                            ATTACH DATABASE '{2}' AS eval;
                         """.format(parameters_db, grid_db, task_db))

    # Get the appropriate records ---------------------------------------------
    with lock:
        # Get the records -----------------------------------------------
        time1 = datetime.now()
        sql="""
        CREATE TABLE all_records (taxon_id TEXT,
                                  record_id PRIMARY KEY,
                                  eventDate TEXT,
                                  weight TEXT,
                                  weight_notes TEXT,
                                  geometry);

        INSERT INTO all_records SELECT taxon_id, record_id, eventDate, weight,
                                       weight_not AS weight_notes, geometry
                                FROM occurrence_records;
        """
        try:
            cursor.executescript(sql)
            conn.commit()
            print("Created a table of all records: " + str(datetime.now()-time1))
        except Exception as e:
            print("!!! FAILED to create a table of all records: {0}".format(str(datetime.now()-time1)))
            print(e)

        sql = """
        SELECT initSpatialMetaData(1);

        CREATE INDEX idx_allss ON all_records (eventDate);

        SELECT RecoverGeometryColumn('all_records', 'geometry', 5070, 'POLYGON', 'XY');

        SELECT CreateSpatialIndex('all_records', 'geometry');
        """
        try:
            cursor.executescript(sql)
            conn.commit()
            print("Created indexes for all records: " + str(datetime.now()-time1))
        except Exception as e:
            print("!!! FAILED to create indexes for all records: " + str(datetime.now()-time1))
            print(e)

    # Intersect records with the grid -----------------------------------------
    intersect(era="all", end_year=time0.year, conn=conn, cursor=cursor)

    # Filter out small fragments ----------------------------------------------
    filter_small(era="all", end_year=time0.year, task_id=task_id,
                 gap_id=gap_id, conn=conn, cursor=cursor)

    sql="""
    /* Add columns */
    ALTER TABLE big_nuff_all ADD COLUMN age_in_weeks INT;
    ALTER TABLE big_nuff_all ADD COLUMN date_assessed INT;

    /* Fill out the days date */
    UPDATE big_nuff_all SET date_assessed = strftime('%Y-%m-%d','now');

    /* Calculate weeks since record */
    UPDATE big_nuff_all
    SET age_in_weeks = (JULIANDAY(date_assessed) - JULIANDAY(eventDate) + 1)/7;
    """
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Added date assessed and age of records : ' + str(datetime.now() - time0))
    except Exception as e:
        print(e)

    sql="""
    /* Choose first in a group by HUC12RNG */
    INSERT INTO eval.last_record SELECT p.strHUC12RNG, bna.record_id,
                                        bna.eventDate, bna.weight,
                                        bna.proportion_circle as proportion_overlap,
                                        MIN(bna.age_in_weeks) as age_in_weeks,
                                        bna.date_assessed, p.geom_5070
                                 FROM big_nuff_all as bna
                                 LEFT JOIN presence as p
                                     ON p.strHUC12RNG = bna.HUC12RNG
        				         GROUP BY p.strHUC12RNG;
    """
    try:
        cursor.executescript(sql)
        conn.commit()
        conn.close()
        del cursor
        print('Filled out last_record table : ' + str(datetime.now() - time0))
    except Exception as e:
        print(e)

    sql = """
    SELECT RecoverGeometryColumn('last_record', 'geom_5070', 5070, 'POLYGON', 'XY');

    SELECT CreateSpatialIndex('last_record', 'geom_5070');

    /* Update layer statistics or else not all columns will show up in QGIS */
    SELECT UpdateLayerStatistics('last_record');
    """
    try:
        cursor, conn = spatialite(task_db)
        cursor.executescript(sql)
        conn.commit()
        conn.close()
        del cursor
        print('Reinstate last_record geometry : ' + str(datetime.now() - time0))
    except Exception as e:
        print(e)

# --------------------------------------------------- Put opinion into a column
def opinion_column(season, start_year, end_year, use_opinions, conn, cursor):
    """
    Column to make note of hucs in presence that have enough evidence.

    PARAMETERS
    ----------
    start_year : integer
    end_year : integer
    conn : spatialite enabled sqlite connection
    cursor : connection cursor
    """
    import sqlite3
    from datetime import datetime

    time1 = datetime.now()
    if use_opinions:
        sql="""
            ALTER TABLE {1} ADD COLUMN opinion_{0} TEXT;
            ALTER TABLE {1} ADD COLUMN opinion_{0}_weight REAL;

            /* Insert rows into table for HUCs that have an opinion but are
            not yet included in table. */
            INSERT INTO {1} (strHUC12RNG)
                SELECT DISTINCT O.hucs
                FROM (SELECT DISTINCT opinions.strHUC12RNG AS hucs
                    FROM opinions WHERE season = '{1}') AS O
                    LEFT JOIN {1} ON {1}.strHUC12RNG = O.hucs
                WHERE {1}.strHUC12RNG IS NULL
                ;
            """.format(str(end_year), season)
    
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Added opinion columns and new rows for {0}: '.format(str(end_year)) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!opinion_column1!!!", end_year)


        sql="""
            UPDATE {3}
            SET opinion_{2} = B.status
            FROM (SELECT MAX(ROWID), strHUC12RNG, status_adjusted AS status
                FROM opinions
                WHERE year BETWEEN {0} AND {1}
                AND season = '{3}'
                GROUP BY strHUC12RNG
                ORDER BY year DESC)
                AS B
            WHERE {3}.strHUC12RNG = B.strHUC12RNG;

            UPDATE {3}
            SET opinion_{2} = 0
            WHERE opinion_{2} = "absent";

            UPDATE {3}
            SET opinion_{2} = 1
            WHERE opinion_{2} = "present";
            """.format(str(start_year), str(end_year), str(end_year), 
                    season)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out most recent opinion between ({0}-{1}): '.format(str(start_year), str(end_year)) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!opinion_column2!!!", end_year)


        sql="""
            UPDATE {3}
            SET opinion_{2}_weight = B.weight
            FROM (SELECT MAX(ROWID), strHUC12RNG, status, weight_adjusted AS weight
                FROM opinions
                WHERE year BETWEEN {0} AND {1}
                AND season = '{3}'
                GROUP BY strHUC12RNG
                ORDER BY year DESC)
                AS B
            WHERE {3}.strHUC12RNG = B.strHUC12RNG;
            """.format(str(start_year), str(end_year), str(end_year),
                    season)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out most recent opinion weight between ({0}-{1}): '.format(str(start_year), str(end_year)) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!opinion_column3!!!", end_year)
        
    else:
        sql="""
            ALTER TABLE {1} ADD COLUMN opinion_{0} TEXT;
            ALTER TABLE {1} ADD COLUMN opinion_{0}_weight REAL;
            """.format(str(end_year), season)
    
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Added empty opinion columns {0}: '.format(str(end_year)) + str(datetime.now() - time1))
        except Exception as e:
            print(e)
            print("!!!opinion_column4!!!", end_year)

# ------------------------------------------------------------- Fill geometries
def fill_new_geometries(season, conn, cursor, grid_db):
    """
    Fill in geometries for newly added subregions

    PARAMETERS
    ----------
    grid_db : string
        Path to the grid sqlite database
    """
    
    season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence", "presence": "presence"}
    season = season_dict[season]
    
    sql = """
    ATTACH DATABASE '{0}' AS shucs;

    UPDATE {1}
    SET geom_5070 = (SELECT geom_5070 FROM huc12rng_gap_polygon
                     WHERE strHUC12RNG = huc12rng_gap_polygon.HUC12RNG)
    WHERE geom_5070 IS NULL;

    /*SELECT RecoverGeometryColumn('{1}', 'geom_5070', 5070, 'POLYGON', 'XY');*/

    /*UPDATE {1}
    SET geom_4326 = Transform(geom_5070, 4326)
    WHERE geom_4326 IS NULL;*/
    """.format(grid_db, season)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Filled out empty geometry columns")
    except Exception as e:
        print(e)

# ------------------------------------------------------------ Compile presence
def compile_presence(task_id, gap_id, task_db, parameters_db, period, era, 
                     grid_db, lock, use_observations, use_opinions):
    """
    Runs other functions to compile presence codes for a time period
    """
    import sqlite3
    import multiprocessing as mp
    from datetime import datetime
    time0 = datetime.now()

    start_year = str(period[0])
    end_year = str(period[1])
    season = "presence"

    # Open a database in memory and attach to data
    #cursor, conn = spatialite("T:/RangeMaps/Development/temp{0}.sqlite".format(periods.index(period)))
    #if period == periods[1]:
    #    cursor, conn = spatialite("T:/RangeMaps/Development/temp1.sqlite")
    #else:
    cursor, conn = spatialite()

    cursor.executescript("""/*Attach databases*/
                            ATTACH DATABASE '{0}' AS params;
                            ATTACH DATABASE '{1}' AS shucs;
                            ATTACH DATABASE '{2}' AS eval;
                            SELECT initSpatialMetaData(1);

                         """.format(parameters_db, grid_db, task_db))
    print("Checking spatial metadata on attached databases")
    print(cursor.execute('SELECT checkSpatialMetaData();').fetchall())


    # Get the appropriate records ---------------------------------------------
    if use_observations:
        with lock:
            get_records(start_year, end_year, conn, cursor, era, 
                        season='presence')

        # Intersect records with the grid -------------------------------------
        intersect(era, end_year, conn, cursor)

        # Filter out small fragments ------------------------------------------
        filter_small(era, end_year, task_id, gap_id, conn, cursor)

        # Add new range subregions --------------------------------------------
        with lock:
            new_subregions(season, era, end_year, conn, cursor)

        # Add a summed weight column ------------------------------------------
        with lock:
            calculate_weight(season, era, end_year, conn, cursor)

    # Document sufficient evidence --------------------------------------------
    with lock:
        set_documented(season, era, conn, cursor, end_year, start_year, 
                       use_observations)

    # Add opinion column ------------------------------------------------------
    if era == 'recent':  
        with lock:
            opinion_column(season, start_year, end_year, use_opinions, conn, 
                           cursor)
    conn.close()

# ------------------------------------------------------ Compile seasonal range
def compile(season, task_id, gap_id, task_db, parameters_db, 
            period, era, grid_db, lock, use_observations, use_opinions):
    """
    Compiles a seasonal range map.  The only difference between year round range and presence is 
    the inclusion of extralimital presence in presence?

    PARAMETERS
    ----------
    season : like "S" or "W" or "Y"
    periods : the tuple of time periods to compile for.
    """
    import sqlite3
    import multiprocessing as mp
    from datetime import datetime
    time0 = datetime.now()
    cursor, conn = spatialite()

    season_dict = {"Y": "year_round", "S": "summer", "W": "winter",
                   "P": "presence"}
    season = season_dict[season]

    start_year = str(period[0])
    end_year = str(period[1])

    cursor.executescript("""/*Attach databases*/
                            ATTACH DATABASE '{0}' AS params;
                            ATTACH DATABASE '{1}' AS shucs;
                            ATTACH DATABASE '{2}' AS eval;
                            SELECT InitSpatialMetaData(1);
                            """.format(parameters_db, grid_db, task_db))

    # Get the appropriate records ---------------------------------------------
    if use_observations:
        with lock:
            get_records(start_year, end_year, conn, cursor, era, season)

        # Intersect records with the grid -------------------------------------
        intersect(era, end_year, conn, cursor)

        # Filter out small fragments ------------------------------------------
        filter_small(era, end_year, task_id, gap_id, conn, cursor)

        # Add new range subregions --------------------------------------------
        with lock:
            new_subregions(season, era, end_year, conn, cursor)

        # Add a summed weight column ------------------------------------------
        with lock:
            calculate_weight(season, era, end_year, conn, cursor)

    # Document sufficient evidence ----------------------------------------
    with lock:
        set_documented(season, era, conn, cursor, end_year, start_year, 
                       use_observations)

    # Add opinion column ------------------------------------------------------
    if era == 'recent':
        with lock:
            opinion_column(season, start_year, end_year, use_opinions, 
                           conn, cursor)
    
    conn.close()

# ---------------------------------------------------------- Simplified Results
def simplified_results(database : str, value_list : list,
                       periods : list) -> None:
    """
    Adds a new table to the task_db database that will hold the results of the
    presence and range tables. Rows for spatial units and columns for presence, 
    and each of the seasonal range maps compiled.  Values will be 1 or NULL
    after converting a user specified range of values to 1 and all others to
    NULL.

    PARAMETERS
    ----------
    database : Path to the database to which the table will be added.
    value_list : List of values to be converted to 1.  All other values will
        be converted to NULL.
    periods : List of years that delineate time periods used in the compilation.

    RETURNS
    -------
    None
    """
    import sqlite3
    import pandas as pd
    seasons = ["summer", "winter", "year_round"]
    years = [x[1] for x in periods]

    # Connect to the database
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    # Determine which seasons have tables in the database, and revise seasons
    # list accordingly
    sql = """SELECT name FROM sqlite_master WHERE type='table';"""
    df = pd.read_sql(sql, conn)
    seasons = [season for season in seasons if season in df.name.values]
    print(seasons)

    # Create a results table with strHUC12RNG as the primary key and that has
    # strHUC12RNG values in it that are in presence or any season tables that
    # exist in the database
    sql = """CREATE TABLE simplified_results (strHUC12RNG TEXT PRIMARY KEY);"""
    cur.execute(sql)
    conn.commit()

    # Add strHUC12RNG values to the simplified_results table
    sql = f"""
    INSERT INTO simplified_results (strHUC12RNG)
    SELECT strHUC12RNG FROM presence;
    """
    cur.executescript(sql)
    conn.commit()

    # Add any strHUC12RNG values from the season tables that are not already
    # in the simplified_results table
    for season in seasons:
        sql = f"""
        INSERT INTO simplified_results (strHUC12RNG)
        SELECT strHUC12RNG FROM {season}
        WHERE strHUC12RNG NOT IN (
            SELECT strHUC12RNG FROM simplified_results
        );
        """
        cur.executescript(sql)
        conn.commit()

    # Add presence values to the simplified_results table
    for year in years:
        sql = f"""
        ALTER TABLE simplified_results ADD COLUMN presence_{year} INTEGER;

        /* Fill out the presence column with 1 where the value in the presence
        table is in the value_list and NULL otherwise */
        UPDATE simplified_results
        SET presence_{year} = 1
        WHERE strHUC12RNG IN (
            SELECT strHUC12RNG FROM presence
            WHERE presence_{year} IN {tuple(value_list)}
        );
        """
        cur.executescript(sql)
        conn.commit()


    # Add season columns to the table if a table exists for the season
    # Loop through the seasons
    for season in seasons:
        # Add columns to the simplified_results table for each year
        for year in years:
            sql = f"""
            ALTER TABLE simplified_results ADD COLUMN {season}_{year} INTEGER;
            """
            cur.execute(sql)
            conn.commit()

            # Fill out the season column with 1 where the value in the corresponding
            # season table is in the value_list and NULL otherwise
            def update_simplified_results(season : str, year : int) -> None:
                sql = f"""
                UPDATE simplified_results
                SET {season}_{year} = 1
                WHERE strHUC12RNG IN (
                    SELECT strHUC12RNG FROM {season}
                    WHERE {season}_{year} IN {tuple(value_list)}
                );
                """
                cur.execute(sql)
                conn.commit()
            
            update_simplified_results(season, year)


    # Close the database connection
    conn.close()

    return None

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
if __name__ == "__main__":
    print("\n**********************************************************",
          "\n**********************************************************")
    timestamp0 = datetime.now()

    # Get parameters
    years, months, error_tolerance, creator, extralimital_m, use_v1, use_observations, use_opinions = get_parameters()

    # Get the existing GAP range data (2001v1)
    if use_v1:
        sb_success, pth = download_GAP_range_CONUS2001v1(gap_id, tmpDir)
    else:
        print("Not using 2001v1 data.")
        sb_success = False

    # Make occurrence record shapefile(s)
    if use_observations:
        for db in ww_output:
            out_file = workDir + "/" + db.split("/")[-1].replace(".sqlite", "")
            occurrence_records(db, out_file)

    # Make the range database for processing and results
    make_range_db(task_db=task_db, gap_id=gap_id, grid_db=grid_db,
                  inDir=tmpDir, workDir=workDir, sb_success=sb_success,
                  seasons=seasons, parameters_db=parameters_db, 
                  use_v1=use_v1, use_observations=use_observations)
    
    # Add a references table
    make_references_table(species=gap_id, task_db=task_db)

    # Insert occurrence records into range database
    if use_observations:
        insert_records(years=years, months=months, task_name=task_name,
                       workDir=workDir, task_db=task_db, codeDir=codeDir)

    # Insert opinion records into range database
    if use_opinions:
        insert_opinions(species=gap_id, seasons=seasons, years=years, 
                        task_db=task_db)

    # # --------------------------- PRESENCE ----------------------------------
    print("\n\tPRESENCE")
    season = 'presence'
    # Create a mutex/lock for writing processes below
    lock = mp.Lock()

    # Kick off processes for each period
    threads_period = []
    for period in periods:
        threads_era = []
        for era in ['recent', 'historical']:
            t = mp.Process(target=compile_presence,
                            args=(task_id, gap_id, task_db, parameters_db,
                                  period, era, grid_db, lock, use_observations, 
                                  use_opinions))
            threads_period.append(t)
            threads_era.append(t)
            t.start()

    # Wait for all threads to finish
    for t in set(threads_era) | set(threads_period):
        t.join()

    # Assess values and determine presence code for the period
    # Connect to the occurrence records database
    try:
        cursor, conn = spatialite(task_db)
    except Exception as e:
        print(e)

    for period in periods:
        assign_code(season, period, periods, conn, cursor)

    # Fill in new geometries
    fill_new_geometries(season, conn, cursor, grid_db)

    # Flag spatial units that are likely beyond the range limit
    for period in periods:
        flag_extralimitals(season, period, conn, cursor, 
                            limit_distance=extralimital_m)

    # Adjust each presence code in light of extralimitals, proximity etc.
    for period in periods:
        adjust_code(season, periods, period, conn, cursor)

    # --------------------------- SEASONS -------------------------------------
    print("\n\tSEASONS")
    # Process seasons if they are requested
    for season in seasons if seasons is not None else []:
        print("\n\t\t{0}".format(season))
        # Create a mutex/lock for writing processes below
        lock = mp.Lock()

        # Kick off processes for each period
        threads_period = []
        for period in periods:
            threads_era = []
            for era in ['recent', 'historical']:
                t = mp.Process(target=compile,
                               args=(season, task_id, gap_id, task_db, 
                                     parameters_db, period, era, grid_db, lock, 
                                     use_observations, use_opinions))
                threads_period.append(t)
                threads_era.append(t)
                t.start()

        # Wait for all threads to finish
        for t in set(threads_era) | set(threads_period):
            t.join()

        # Assess values and determine presence code for the period
        # Connect to the occurrence records database
        try:
            cursor, conn = spatialite(task_db)
        except Exception as e:
            print(e)

        for period in periods:
            assign_code(season, period, periods, conn, cursor)

        # Fill in new geometries
        fill_new_geometries(season, conn, cursor, grid_db)

        # Flag spatial units that are likely beyond the range limit
        for period in periods:
            flag_extralimitals(season, period, conn, cursor, 
                                limit_distance=extralimital_m)

        # Adjust each presence code in light of extralimitals, proximity etc.
        for period in periods:
            adjust_code(season, periods, period, conn, cursor)

    # ------------------------- LAST RECORD -----------------------------------
    if use_observations:
        # Calculate age of last record
        last_record(task_id, gap_id, task_db, parameters_db, workDir, codeDir,
                    grid_db, lock)

    conn.close()
    del lock

    # ---------------------- SIMPLIFIED RESULTS -------------------------------
    # Make a table of simplified results with 1 and NULL values
    simplified_results(task_db, [1,2,3], periods)

    # Total runtime
    print("Total runtime: " + str(datetime.now() - timestamp0))