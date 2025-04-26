"""
Some useful functions for using the GAP-range-compiler.
"""
# ------------------------------------------------------ GAP HUC sqlite database
def make_spatialite_hucs(huc_shp, out_db):
    """
    Create a spatialite database from GAP's huc12's.

    (huc_shp, out_db) --> new file at out_db

    PARAMETERS
    ----------
    huc_shp : string
        path to a huc12rng shapefile to use.  Do no provide the ".shp" suffix.
    out_db : string
        path of sqlite database to be created.
    """
    import os
    import sqlite3

    # Create the database
    cursor, connection = spatialite(out_db)
    cursor.execute("SELECT InitSpatialMetadata(1);")

    # Add hucs -----------------------------------------------------------------
    try:
        cursor.execute("""SELECT ImportSHP(?, 'huc12rng_gap_polygon',
                                        'utf-8', 5070, 'geom_5070',
                                        'HUC12RNG', 'POLYGON');""", (huc_shp,))
    except Exception as e:
        print(e)

    # Add indices --------------------------------------------------------------
    try:
        sql = """CREATE INDEX idx_shuc ON huc12rng_gap_polygon (HUC12RNG);
                 SELECT CreateSpatialIndex('huc12rng_gap_polygon', 'geom_5070');"""
        cursor.executescript(sql)
    except Exception as e:
        print(e)

    connection.close()
    
def spatialite(db=":memory:"):
    """
    Creates a connection and cursor for sqlite db and enables spatialite
        extension and shapefile functions.  Defaults to in-memory database.

    (db) --> cursor, connection

    Arguments:
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

def download_GAP_range_CONUS2001v1(gap_id, toDir):
    """
    Downloads GAP Range CONUS 2001 v1 file and returns path to the unzipped
    file.  NOTE: doesn't include extension in returned path so that you can
    specify if you want csv or shp or xml when you use the path.
    """
    import sciencebasepy
    import zipfile

    # Connect
    sb = sciencebasepy.SbSession()

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
    return rng_zip.replace('.zip', '')

def make_evaluation_db(eval_db, gap_id, inDir, outDir, huc_db):
    """
    Builds an sqlite database in which to store range evaluation information.

    Tables created:
    range_2001v1 -- the range data downloaded from ScienceBase.
    presence_2020v1 -- where data on predicted and documented presence is stored.

    Arguments:
    eval_db -- name of database to create for evaluation.
    gap_id -- gap species code. For example, 'bAMROx'
    huc_db -- path to GAP's 12 digit hucs in sqlite/spatialite format
    inDir -- project's input directory
    outDir -- output directory for this repo
    """
    import sqlite3
    import pandas as pd
    import os

    # Delete db if it exists
    if os.path.exists(eval_db):
        os.remove(eval_db)

    # Create the database
    cursorQ, conn = spatialite(eval_db)

    cursorQ.execute('SELECT InitSpatialMetadata(1);')

    ###################### ADD 2001v1 RANGE
    csvfile = inDir + gap_id + "_CONUS_RANGE_2001v1.csv"
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

    /*  Set a primary key -- this is cumbersome due to sqlite3/pandas limitations.*/
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

    CREATE INDEX idx_range2001_HUC ON range_2001v1 (strHUC12RNG);
    """
    cursorQ, conn = spatialite(eval_db)
    cursorQ.executescript(sql1)
    # # Table to use for evaluations, renamed from 'presence' 14April2020.
    # sql2 = """
    # ATTACH DATABASE '{0}' AS hucs;
    #
    # CREATE TABLE presence_2001v1 AS SELECT range_2001v1.strHUC12RNG, shucs.geom_5070
    #                          FROM range_2001v1 LEFT JOIN huc12rng_gap_polygon as shucs
    #                                            ON range_2001v1.strHUC12RNG = shucs.HUC12RNG;
    #
    # CREATE INDEX idx_pres01v1_HUC ON presence_2001v1 (strHUC12RNG);
    #
    # /* Transform to 4326 for displaying purposes*/
    # /*ALTER TABLE presence_2001v1 ADD COLUMN geom_4326 INTEGER;
    #
    # UPDATE presence_2001v1 SET geom_4326 = Transform(geom_5070, 4326);
    #
    # SELECT RecoverGeometryColumn('presence_2001v1', 'geom_4326', 4326, 'POLYGON', 'XY');
    #
    # SELECT ExportSHP('presence_2001v1', 'geom_4326', '{1}{2}_presence2001_4326', 'utf-8');*/
    # """.format(huc_db, outDir, gap_id)
    # cursorQ.executescript(sql2)

    # Create a table to store presence information for range compilations.

    sql = """
    ATTACH DATABASE '{0}' AS hucs;

    CREATE TABLE presence AS SELECT range_2001v1.strHUC12RNG,
                                    range_2001v1.intGAPPresence AS presence_2001v1,
                                    shucs.geom_5070
                             FROM range_2001v1 LEFT JOIN hucs.huc12rng_gap_polygon as shucs
                                               ON range_2001v1.strHUC12RNG = shucs.HUC12RNG;
    """.format(huc_db)
    try:
        cursorQ.executescript(sql)
    except Exception as e:
        print(e)

    sql = """
    /* Set a primary key */
    BEGIN TRANSACTION;
    ALTER TABLE presence RENAME TO garbage3;

    /*create a new table with the same column names and types while
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
    CREATE INDEX idx_presence_HUC ON presence (strHUC12RNG);

    SELECT RecoverGeometryColumn('presence', 'geom_5070', 5070, 'POLYGON',
                                 'XY');
    """
    try:
        cursorQ.executescript(sql)
    except Exception as e:
        print(e)
    conn.commit()
    conn.close()
    del cursorQ
    # /* Transform to 4326 for displaying purposes*/
    # ALTER TABLE presence ADD COLUMN geom_4326 INTEGER;
    #
    # UPDATE presence SET geom_4326 = Transform(geom_5070, 4326);
    #
    # SELECT RecoverGeometryColumn('presence', 'geom_4326', 4326, 'POLYGON',
    #                              'XY');
    #
    # SELECT ExportSHP('presence', 'geom_4326', '{0}{1}_presence_4326', 'utf-8');

def insert_records(years, months, summary_name, outDir, eval_db, codeDir):
    '''
    For the notebook.  Gets records from occurrence dbs and puts
    them into the evaluation db.
    '''

    # Connect to the evaluation occurrence records database
    try:
        cursor, evconn = spatialite(eval_db)
    except Exception as e:
        print(e)

    # Get records with geometry column, save output for loading into sqlite below
    SHP = outDir + summary_name + "_footprints/" + summary_name + "_footprints"

    print(SHP)
    import os
    print(os.environ["SPATIALITE_SECURITY"])
    try:
        cursor.execute("""SELECT ImportSHP(?, 'evaluation_occurrences',
                           'UTF-8', 5070, 'geometry', 'index', 'POLYGON');""", (SHP,))
    except Exception as e:
        print(e)

    try:
        sql = """
        CREATE INDEX idx_eo_date ON evaluation_occurrences (eventDate);
        CREATE INDEX idx_eo_id ON evaluation_occurrences (record_id);
        """
        cursor.executescript(sql)
    except Exception as e:
        print(e)

    # Register the geometry column
    cursor.execute("""SELECT RecoverGeometryColumn('evaluation_occurrences', 'geometry',
                      5070, 'POLYGON', 'XY');""")


    '''# Filter out bad years and months
    df2 = (df1
           [lambda x: datetime.strptime(x["eventDate"], "%Y-%m-%dT%H:%M:%S").year.isin(years) == True]
           [lambda x: datetime.strptime(x["eventDate"], "%Y-%m-%dT%H:%M:%S").month.isin(months) == True]
           )'''

    '''# Drop records with years and months not wanted
    cursor.execute("""DELETE FROM evaluation_occurrences
                       WHERE STRFTIME('%Y', eventDate) NOT IN {0}
                       AND STRFTIME('%m', eventDate) NOT IN {1};""".format(years, months))
                       '''
    # Close db
    evconn.commit()
    evconn.close()
    '''
            # Attach occurrence database
            cursor.execute("ATTACH DATABASE ? AS occs;", (occ_db,))

            # Create table of occurrences that fit within evaluation parameters  --  IF EXISTS JUST APPEND
            if occ_db == occ_dbs[0]:
                cursor.execute("""CREATE TABLE evaluation_occurrences AS
                               SELECT * FROM occs.occurrence_records
                               WHERE STRFTIME('%Y', eventDate) IN {0}
                               AND STRFTIME('%m', eventDate) IN {1};""".format(years, months))
            else:
                cursor.execute("""INSERT INTO evaluation_occurrences
                                  SELECT * FROM occs.occurrence_records
                                  WHERE STRFTIME('%Y', eventDate) IN {0}
                                  AND STRFTIME('%m', eventDate) IN {1};""".format(years, months))


    # Export occurrence circles as a shapefile (all seasons)
    cursor.execute("""SELECT RecoverGeometryColumn('evaluation_occurrences', 'polygon_4326',
                      4326, 'POLYGON', 'XY');""")
    sql = """SELECT ExportSHP('evaluation_occurrences', 'polygon_4326', ?, 'utf-8');"""
    subs = outDir + summary_name + "_circles"
    cursor.execute(sql, (subs,))

    # Export occurrence 'points' as a shapefile (all seasons)
    cursor.execute("""SELECT RecoverGeometryColumn('evaluation_occurrences', 'geom_xy4326',
                      4326, 'POINT', 'XY');""")
    subs = outDir + summary_name + "_points"
    cursor.execute("""SELECT ExportSHP('evaluation_occurrences', 'geom_xy4326', ?, 'utf-8');""", (subs,))
    '''

# def start_compiling(periods, gap_id, eval_db, parameters_db, outDir, codeDir, eval_id, grid_db):
#     """
#     Startup range compilation for each of the seasons.
#
#     PARAMETERS
#     ----------
#     periods : tuple of tuples
#         specification of how to break 2000-present into time periods. Each period
#         should be a tuple of start and end year (inclusive, e.g., (2000,2004)
#     """
#     from datetime import datetime
#     import multiprocessing as mp
#     print(__name__)
#     if __name__ == "main":
#
#     Class compile_period(mp.Process):
#         def __init__(self, eval_id, gap_id, eval_db, parameters_db,
#                      outDir, codeDir, period, grid_db):
#             super().__init__()
#
#         def run(self):
#             compile_GAP_presence(eval_id, gap_id, eval_db, parameters_db,
#                                  outDir, codeDir, period, grid_db)
#
#         # Create a mutex/lock for writing processes below
#         lock = mp.Lock()
#
#         # def compile():
#         #     compile_GAP_presence(eval_id, gap_id, eval_db, parameters_db,
#         #                          outDir, codeDir, period, grid_db)
#
#         # Kick off processes for each period
#         thread_list = []
#         for period in periods:
#             t = compile_period()
#             thread_list.append(t)
#             t.start()
#
#         # wait for all threads to finish
#         for t in thread_list:
#             t.join()
#
#         # # Compile presence for each column
#         # for p in periods:
#         #     compile_GAP_presence(eval_id, gap_id, eval_db, parameters_db,
#         #                          outDir, codeDir, p, grid_db)
#
#         # Connect to the evaluation database
#         cursor, conn = spatialite(eval_db)
#
#         # Calculate the years since a record, if current year is in the period
#         if datetime.now().year in range(period[0], period[1]):
#              years_since(period, conn, cursor, version)
#
#         # Fill in new geometries
#         fill_new_geometries(conn, cursor, grid_db)
#         conn.close()

def get_records(start_year, conn, cursor, era):
    """
    Get the appropriate species occurrence records to use.

    PARAMETERS
    ----------
    start_year : integer
    cursor : sqlite3 cursor
    era : string
        'recent' or 'historical'
    """
    from datetime import datetime

    if era == 'recent':
        condition = '>=' + str(start_year)
    else:
        condition = '<' + str(start_year)

    # Get the records -----------------------------------------------
    time1 = datetime.now()
    sql="""
    CREATE TABLE {0}_records (taxon_id TEXT,
                              record_id PRIMARY KEY,
                              eventDate TEXT,
                              weight TEXT,
                              weight_notes TEXT,
                              geometry);

    INSERT INTO {0}_records SELECT taxon_id, record_id, eventDate, weight,
                                   weight_not AS weight_notes, geometry
                            FROM evaluation_occurrences
                            WHERE eventDate {1};
    """.format(era, condition)
    try:
        cursor.executescript(sql)
        conn.commit()
        print(start_year,era,"Created a table of {0} records: ".format(era) + str(datetime.now()-time1))
    except Exception as e:
        print("!!! FAILED to create a table of {0} records: ".format(era) + str(datetime.now()-time1))
        print(e)

    sql = """
    CREATE INDEX idx_{0}s ON {0}_records (eventDate);

    SELECT RecoverGeometryColumn('{0}_records', 'geometry', 5070, 'POLYGON', 'XY');

    SELECT CreateSpatialIndex('{0}_records', 'geometry');
    """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Created indexes {0} records: ".format(era) + str(datetime.now()-time1))
    except Exception as e:
        print("!!! FAILED to create indexes for {0} records: ".format(era) + str(datetime.now()-time1))
        print(e)

def intersect(era, conn, cursor):
    """
    Intersects occurrence records and the grid

    PARAMETERS
    ---------
    era : string
        'recent' or 'historical'
    """
    from datetime import datetime
    time1 = datetime.now()

    sql="""
    CREATE TABLE intersected_{0} (HUC12RNG TEXT,
                                  record_id TEXT,
                                  eventDate TEXT,
                                  weight TEXT,
                                  geom_5070);

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

    /* Choice of 'MULTIPOLYGON' here is important. */
    SELECT RecoverGeometryColumn('intersected_{0}', 'geom_5070', 5070,
                                 'MULTIPOLYGON', 'XY');

    CREATE INDEX idx_intersect_{0}s ON intersected_{0} (HUC12RNG, record_id, eventDate, weight);
    """.format(era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Found hucs that intersect a {0} occurrence: ".format(era) + str(datetime.now()-time1))
    except Exception as e:
        print("!! FAILED to find hucs that intersect a {0} occurrence: ".format(era) + str(datetime.now()-time1))
        print(e)

def filter_small(era, eval_id, gap_id, conn, cursor):
    """
    Use the error tolerance for the species to select those occurrences that
    can be attributed to a HUC.

    PARAMETERS
    ----------
    eval_id : string
        The name of the evaluation database
    gap_id : string
        The GAP code of the species
    era : string
        "historical" or "recent"
    cursor : sqlite3 cursor

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
                               proportion_circle);


    INSERT INTO big_nuff_{2} SELECT intersected_{2}.HUC12RNG,
                                    intersected_{2}.record_id,
                                    intersected_{2}.eventDate,
                                    intersected_{2}.weight,
                                    100 * (ST_Area(intersected_{2}.geom_5070) / ST_Area(eo.geometry))
                                        AS proportion_circle
                             FROM intersected_{2}
                                  LEFT JOIN evaluation_occurrences AS eo
                                  ON intersected_{2}.record_id = eo.record_id
                             WHERE proportion_circle BETWEEN (100 - (SELECT error_tolerance
                                                                     FROM params.evaluations
                                                                     WHERE evaluation_id = '{0}'
                                                                     AND species_id = '{1}'))
                                                     AND 100
                             ORDER BY proportion_circle ASC;

      CREATE INDEX idx_bn_{2} ON big_nuff_{2} (HUC12RNG, record_id);
    """.format(eval_id, gap_id, era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined which records overlap enough: ' + str(datetime.now() - time1))
    except Exception as e:
        print(e)

def calculate_weight(era, end_year, version, conn, cursor):
    """
    Column to make note of hucs in presence that have enough evidence.

    PARAMETERS
    ----------
    era : string
        'recent' or 'historical'
    end_year : integer
    version : string
    cursor : sqlite3 cursor
    """
    import sqlite3
    from datetime import datetime

    time1 = datetime.now()
    sql="""
    ALTER TABLE presence ADD COLUMN {1}_weight_{0} INT;

    UPDATE presence
    SET {1}_weight_{0} = (SELECT SUM(weight)
                          FROM big_nuff_{1}
                          WHERE HUC12RNG = presence.strHUC12RNG
                          GROUP BY HUC12RNG);
        """.format(str(end_year) + version, era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Calculated total weight of evidence for each huc : ' + str(datetime.now() - time1))
    except Exception as e:
        print(e)

def new_subregions(era, end_year, version, conn, cursor):
    """
    Find hucs that contained gbif occurrences, but were not in gaprange and
    insert them into the presence table as new records.

    PARAMETERS
    ----------
    era : string
        'recent' : 'historical'
    end_year : integer
    version : string
    """
    from datetime import datetime

    time1 = datetime.now()
    sql="""
    INSERT INTO presence (strHUC12RNG, {1}_weight_{0})
                SELECT big_nuff_{1}.HUC12RNG, SUM(big_nuff_{1}.weight)
                FROM big_nuff_{1} LEFT JOIN presence
                                        ON presence.strHUC12RNG = big_nuff_{1}.HUC12RNG
                WHERE presence.strHUC12RNG IS NULL
                GROUP BY big_nuff_{1}.HUC12RNG;
    """.format(str(end_year) + version, era)
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Added rows for hucs with enough weight but not in GAP range : ' + str(datetime.now() - time1))
    except Exception as e:
        print(e)

def set_documented(era, conn, cursor, end_year, start_year, version):
    """
    Mark records/subregions that have sufficient evidence of presence

    PARAMETERS
    ----------
    era : string
        'recent' : 'historical'
    end_year : integer
    version : string
    """
    import sqlite3
    from datetime import datetime

    time1 = datetime.now()
    if era == 'recent':
        sql="""
            ALTER TABLE presence ADD COLUMN documented_{0} INT;

            UPDATE presence SET documented_{0} = 1 WHERE recent_weight_{0} >= 10;
            """.format(str(end_year) + version)
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out documented recent column : ' + str(datetime.now() - time1))
        except Exception as e:
            print(e)

    if era == 'historical':
        sql="""
            ALTER TABLE presence ADD COLUMN documented_pre{1} INT;

            UPDATE presence SET documented_pre{1} = 1 WHERE historical_weight_{0} >= 10;
            """.format(str(end_year) + version, str(start_year))
        try:
            cursor.executescript(sql)
            conn.commit()
            print('Filled out documented historical column : ' + str(datetime.now() - time1))
        except Exception as e:
            print(e)

def presence_code(period, conn, cursor, version):
    """
    """
    from datetime import datetime
    start_year = str(period[0])
    end_year = str(period[1])

    ##########################################  Fill out new presence column
    ########################################################################
    time1 = datetime.now()
    sql="""
    /* Add columns */
    ALTER TABLE presence ADD COLUMN presence_{0} INT;

    /* NOTE: The order of these statements matters and reflects their rank */
    UPDATE presence SET presence_{0} = presence_2001v1;

    /* Reclass some values */
    UPDATE presence SET presence_{0} = 3 WHERE presence_{0} in (1,2,3);
    UPDATE presence SET presence_{0} = 3 WHERE documented_pre{1}=1;
    UPDATE presence SET presence_{0} = 1 WHERE documented_{0}=1;
    """.format(str(end_year) + version, str(start_year))
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined {0} range presence value : '.format(str(end_year) + version) + str(datetime.now() - time1))
    except Exception as e:
        print(e)

def years_since(conn, cursor, version):
    """
    """
    print("-----Starting years since")
    from datetime import datetime

    sql="""
    /* Add column */
    ALTER TABLE presence ADD COLUMN yrs_since_record INT;

    /* Combine big nuff tables into one */
    CREATE TABLE all_big_nuff AS SELECT * FROM big_nuff_recent;

    INSERT INTO all_big_nuff SELECT * FROM big_nuff_historical;

    /* Calculate years since record in a new column */
    ALTER TABLE all_big_nuff ADD COLUMN years_since INT;

    UPDATE all_big_nuff
    SET years_since = strftime('%Y', 'now') - strftime('%Y', eventDate);

    /* Choose first in a group by HUC12RNG */
    UPDATE presence
    SET yrs_since_record = (SELECT MIN(years_since)
    				  FROM all_big_nuff
    				  WHERE HUC12RNG = presence.strHUC12RNG
    				  GROUP BY HUC12RNG);

    /* Replace null values with a dummy value */
    UPDATE presence SET yrs_since_record = 999 WHERE yrs_since_record IS NULL;

    /* Update layer statistics or else not all columns will show up in QGIS */
    SELECT UpdateLayerStatistics('presence');
    """
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Determined age of last occurrence : ' + str(datetime.now() - time1))
    except Exception as e:
        print(e)

def fill_new_geometries(conn, cursor, grid_db):
    """
    """
    print("-----fill_new_geometries")
    ###############################  Fill in geometry values for "new" HUCs
    #######################################################################
    sql = """
    ATTACH DATABASE '{0}' AS shucs;

    UPDATE presence
    SET geom_5070 = (SELECT geom_5070 FROM huc12rng_gap_polygon
                     WHERE strHUC12RNG = huc12rng_gap_polygon.HUC12RNG)
    WHERE geom_5070 IS NULL;

    /*SELECT RecoverGeometryColumn('presence', 'geom_5070', 5070, 'POLYGON', 'XY');*/

    /*UPDATE presence
    SET geom_4326 = Transform(geom_5070, 4326)
    WHERE geom_4326 IS NULL;*/
    """.format(grid_db)
    try:
        cursor.executescript(sql)
        conn.commit()
        print("Filled out empty geometry columns")
    except Exception as e:
        print(e)

def compile_GAP_presence(eval_id, gap_id, eval_db, parameters_db, outDir,
                         codeDir, period, era, grid_db, lock, cursor, conn):
    """
    Uses occurrence data collected with the wildlife-wrangler repo
    to build an updated GAP range map for a species.  The previous GAP range
    is used along with recent and historic occurrence records acquired with
    the wildlife-wrangler.

    The results of this code are a new column in the GAP range table (in the db
    created for the task) and a range shapefile.

    PARAMETERS
    ----------
    eval_id : string
        name/code for the update (e.g., 2020v1)
    gap_id : string
        gap species code.
    eval_db : string
        path to the evaluation database.  It should have been created with
        make_evaluation_db() so the schema is correct.
    start_year : int
        start years to define the "current" time period.  Inclusive:
        (2000,2004) would denote a five year period of 2000, 2001, ..., 2005.
    end_year : int
        end years to define the "current" time period.  Inclusive:
        (2000,2004) would denote a five year period of 2000, 2001, ..., 2005.
    era : string

    parameters_db : string
        database with information on range update and evaluation criteria.
    grid : string
        path to the grid sqlite, such as huc12rng_gap_polygon.sqlite.
    outDir : string
        directory where to put output
    codeDir : string
        directory of code repo
    lock :

    """
    import sqlite3
    import os
    import multiprocessing as mp
    from datetime import datetime
    time0 = datetime.now()

    print(period, era)
    start_year = str(period[0])
    end_year = str(period[1])
    version = "v1"

    ############################### Open a database in memory and attach to data
    ############################################################################
    if period == periods[1]:
        cursor, conn = spatialite()#"T:/RangeMaps/Development/temp2.sqlite")
    else:
        cursor, conn = spatialite()

    cursor.executescript("""/*Attach databases*/
                            ATTACH DATABASE '{0}' AS params;
                            ATTACH DATABASE '{1}' AS shucs;
                            ATTACH DATABASE '{2}' AS eval;
                         """.format(parameters_db, grid_db, eval_db))

    ########################################## Find recently occupied subregions
    ############################################################################
    """
    Get a table with occurrences from the right time period with the names of
    hucs that they intersect (proportion in polygon assessement comes later).
    intersected_recent -- occurrences of suitable age and the hucs they intersect at all.
             Records are fragments of circles after intersection with hucs.
    This ultimately populates the documented_recent column.
    """
    # Get the appropriate records -----------------------------------------------
    with lock:
        get_records(start_year, conn, cursor, era)

    # Intersect records with the grid ---------------------------------------
    with lock:
        intersect(era, conn, cursor)

    # Filter out small fragments --------------------------------------------
    with lock:
        filter_small(era, eval_id, gap_id, conn, cursor)

    # Add a summed weight column ------------------------------------------- WRITE
    with lock:
        calculate_weight(era, end_year, version, conn, cursor)

    # Add new range subregions --------------------------------------------- WRITE
    with lock:
        new_subregions(era, end_year, version, conn, cursor)

    # Document sufficient evidence ----------------------------------------- WRITE
    with lock:
        set_documented(era, conn, cursor, end_year, start_year, version)

    conn.close()

    # #####################################  Export shapefile for notebook (4326)
    # ########################################################################
    # time1 = datetime.now()
    # sql="""
    # CREATE TABLE out AS SELECT geom_4326, yrs_since_record AS age, presence_{2} AS presence
    #                     FROM presence;
    # SELECT RecoverGeometryColumn('out', 'geom_4326', 4326, 'POLYGON', 'XY');
    # SELECT ExportSHP('out', 'geom_4326', '{0}{1}NB', 'utf-8');
    # DROP TABLE out;
    # """.format(outDir, gap_id, str(end_year) + version)
    # try:
    #     cursor.executescript(sql)
    #     print('Exported shapefile : ' + str(datetime.now() - time1))
    # except Exception as e:
    #     print(e)



# def cleanup_eval_db(eval_db):
#     '''
#     Drop excess tables and columns to reduce file size.
#     '''
#     from datetime import datetime
#     time1 = datetime.now()
#     cursor, conn = spatialite(eval_db)
#     sql="""
#     DROP TABLE all_big_nuff;
#     DROP TABLE big_nuff_historical;
#     DROP TABLE big_nuff_recent;
#     DROP TABLE historical_records;
#     DROP TABLE recent_records;
#     DROP TABLE intersected_historical;
#     DROP TABLE intersected_recent;
#     DROP TABLE shucs;
#     DROP TABLE presence_2001v1;
#     DROP TABLE range_2001v1;
#
#     /* Get rid of the geom_4326 column */
#     CREATE TABLE IF NOT EXISTS new_pres AS
#                 SELECT strHUC12RNG, presence_2001v1, documented_historical,
#                        documented_recent, yrs_since_record, presence_{0},
#                        geom_5070
#                 FROM presence;
#
#     DROP TABLE presence;
#
#     ALTER TABLE new_pres RENAME TO presence;
#
#     SELECT RecoverGeometryColumn('presence', 'geom_5070', 5070, 'MULTIPOLYGON',
#                                  'XY');
#     """
#     try:
#         cursor.executescript(sql)
#         print('Deleted excess tables and columns : ' + str(datetime.now() - time1))
#     except Exception as e:
#         print(e)
#
#     conn.commit()
#     conn.close()

def MapShapefilePolygons(map_these, title):
    """
    Displays shapefiles on a simple CONUS basemap.  Maps are plotted in the order
    provided so put the top map last in the listself.  You can specify a column
    to map as well as custom colors for it.  This function may not be very robust
    to other applications.

    NOTE: The shapefiles have to be in WGS84 CRS.

    (list, str) -> displays maps, returns matplotlib.pyplot figure

    Arguments:
    map_these -- list of dictionaries for shapefiles you want to display in
                CONUS. Each dictionary should have the following format, but
                some are unneccesary if 'column' doesn't = 'None'.  The critical
                ones are file, column, and drawbounds.  Column_colors is needed
                if column isn't 'None'.  Others are needed if it is 'None'.
                    {'file': '/path/to/your/shapfile',
                     'alias': 'my layer'
                     'column': None,
                     'column_colors': {0: 'k', 1: 'r'}
                    'linecolor': 'k',
                    'fillcolor': 'k',
                    'linewidth': 1,
                    'drawbounds': True
                    'marker': 's'}
    title -- title for the map.
    """
    # Packages needed for plotting
    import matplotlib.pyplot as plt
    from mpl_toolkits.basemap import Basemap
    import numpy as np
    from matplotlib.patches import Polygon
    from matplotlib.collections import PatchCollection
    from matplotlib.patches import PathPatch

    # Basemap
    fig = plt.figure(figsize=(15,12))
    ax = plt.subplot(1,1,1)
    map = Basemap(projection='aea', resolution='l', lon_0=-95.5, lat_0=39.0, height=3200000, width=5000000)
    map.drawcoastlines(color='grey')
    map.drawstates(color='grey')
    map.drawcountries(color='grey')
    map.fillcontinents(color='#a2d0a2',lake_color='#a9cfdc')
    map.drawmapboundary(fill_color='#a9cfdc')

    for mapfile in map_these:
        if mapfile['column'] == None:
            # Add shapefiles to the map
            if mapfile['fillcolor'] == None:
                map.readshapefile(mapfile['file'], 'mapfile',
                                  drawbounds=mapfile['drawbounds'],
                                  linewidth=mapfile['linewidth'],
                                  color=mapfile['linecolor'])
                # Empty scatter plot for the legend
                plt.scatter([], [], c='', edgecolor=mapfile['linecolor'],
                            alpha=1, label=mapfile['alias'], s=100,
                            marker=mapfile['marker'])

            else:
                map.readshapefile(mapfile['file'], 'mapfile',
                          drawbounds=mapfile['drawbounds'])
                # Code for extra formatting -- filling in polygons setting border
                # color
                patches = []
                for info, shape in zip(map.mapfile_info, map.mapfile):
                    patches.append(Polygon(np.array(shape), True))
                ax.add_collection(PatchCollection(patches,
                                                  facecolor= mapfile['fillcolor'],
                                                  edgecolor=mapfile['linecolor'],
                                                  linewidths=mapfile['linewidth'],
                                                  zorder=2))
                # Empty scatter plot for the legend
                plt.scatter([], [], c=mapfile['fillcolor'],
                            edgecolors=mapfile['linecolor'],
                            alpha=1, label=mapfile['alias'], s=100,
                            marker=mapfile['marker'])

        else:
            map.readshapefile(mapfile['file'], 'mapfile', drawbounds=mapfile['drawbounds'])
            for info, shape in zip(map.mapfile_info, map.mapfile):
                for thang in mapfile['column_colors'].keys():
                    if info[mapfile['column']] == thang:
                        x, y = zip(*shape)
                        map.plot(x, y, marker=None, color=mapfile['column_colors'][thang])

            # Empty scatter plot for the legend
            for seal in mapfile['column_colors'].keys():
                plt.scatter([], [], c=mapfile['column_colors'][seal],
                            edgecolors=mapfile['column_colors'][seal],
                            alpha=1, label=mapfile['value_alias'][seal],
                            s=100, marker=mapfile['marker'])

    # Legend -- the method that works is ridiculous but necessary; you have
    #           to add empty scatter plots with the symbology you want for
    #           each shapefile legend entry and then call the legend.  See
    #           plt.scatter(...) lines above.
    plt.legend(scatterpoints=1, frameon=True, labelspacing=1, loc='lower left',
               framealpha=1, fontsize='x-large')

    # Title
    plt.title(title, fontsize=20, pad=-40, backgroundcolor='w')
    return


# def evaluate_GAP_range(eval_id, gap_id, eval_db, parameters_db, outDir, codeDir):
    # """
    # Uses occurrence data collected with the wildlife-wrangler repo
    # to evaluate the GAP range map for a species.  A table is created for the GAP
    # range and columns reporting the results of evaluation and validation are
    # populated after evaluating spatial relationships of occurrence records (circles)
    # and GAP range.
    #
    # The results of this code are new columns in the GAP range table (in the db
    # created for work in this repository) and a range shapefile.
    #
    # The primary use of code like this would be range evaluation and revision.
    #
    # Unresolved issues:
    # 1. Can the runtime be improved with spatial indexing?  Minimum bounding rectangle?
    # 3. Locations of huc files. -- can sciencebase be used?
    # 4. Condition data used on the parameters, such as filter_sets in the evaluations
    #    table.
    #
    # Arguments:
    # eval_id -- name/code of the evaluation
    # gap_id -- gap species code.
    # eval_db -- path to the evaluation database.  It should have been created with
    #             make_evaluation_db() so the schema is correct.
    # parameters_db -- database with information on range update and evaluation
    #             criteria.
    # outDir -- directory of
    # codeDir -- directory of code repo
    # """
    # import sqlite3
    # import os
    #
    # cursor, conn = spatialite(parameters_db)
    # method = cursor.execute("""SELECT method
    #                            FROM evaluations
    #                            WHERE evaluation_id = ?;""",
    #                            (eval_id,)).fetchone()[0]
    # conn.close()
    # del cursor
    #
    # # Range evaluation database.
    # cursor, conn = spatialite(eval_db)
    # cursor.executescript("""ATTACH DATABASE '{0}'
    #                         AS params;""".format(parameters_db))
    #
    # sql2="""
    # /*#############################################################################
    #                              Assess Agreement
    #  ############################################################################*/
    #
    # /*#########################  Which HUCs contain an occurrence?
    #  #############################################################*/
    # /*  Intersect occurrence circles with hucs  */
    # CREATE TABLE intersected_recent AS
    #               SELECT shucs.HUC12RNG, eo.record_id,
    #               CastToMultiPolygon(Intersection(shucs.geom_5070,
    #                                               eo.geometry)) AS geom_5070
    #               FROM shucs, evaluation_occurrences AS eo
    #               WHERE Intersects(shucs.geom_5070, eo.geometry);
    #
    # SELECT RecoverGeometryColumn('intersected_recent', 'geom_5070', 5070, 'MULTIPOLYGON',
    #                              'XY');
    #
    # /* In light of the error tolerance for the species, which occurrences can
    #    be attributed to a huc?  */
    # CREATE TABLE big_nuff_recent AS
    #   SELECT intersected_recent.HUC12RNG, intersected_recent.record_id,
    #          100 * (Area(intersected_recent.geom_5070) / Area(eo.geometry))
    #             AS proportion_circle
    #   FROM intersected_recent
    #        LEFT JOIN evaluation_occurrences AS eo
    #        ON intersected_recent.record_id = eo.record_id
    #   WHERE proportion_circle BETWEEN (100 - (SELECT error_tolerance
    #                                           FROM params.evaluations
    #                                           WHERE evaluation_id = '{0}'
    #                                           AND species_id = '{1}'))
    #                           AND 100;
    #
    # /*  How many occurrences in each huc that had an occurrence? */
    # ALTER TABLE sp_range ADD COLUMN weight_sum INT;
    #
    # UPDATE sp_range
    # SET weight_sum = (SELECT SUM(weight)
    #                       FROM big_nuff_recent
    #                       WHERE HUC12RNG = sp_range.strHUC12RNG
    #                       GROUP BY HUC12RNG);
    #
    #
    # /*  Find hucs that contained gbif occurrences, but were not in gaprange and
    # insert them into sp_range as new records.  Record the occurrence count */
    # INSERT INTO sp_range (strHUC12RNG, weight_sum)
    #             SELECT big_nuff_recent.HUC12RNG, SUM(weight)
    #             FROM big_nuff_recent LEFT JOIN sp_range ON sp_range.strHUC12RNG = big_nuff_recent.HUC12RNG
    #             WHERE sp_range.strHUC12RNG IS NULL
    #             GROUP BY big_nuff_recent.HUC12RNG;
    #
    #
    # /*############################  Does HUC contain enough weight?
    # #############################################################*/
    # ALTER TABLE sp_range ADD COLUMN eval INT;
    #
    # /*  Record in sp_range that gap and gbif agreed on species presence, in light
    # of the minimum weight of 10 */
    # UPDATE sp_range
    # SET eval = 1
    # WHERE weight_sum >= 10;
    #
    #
    # /*  For new records, put zeros in GAP range attribute fields  */
    # UPDATE sp_range
    # SET intGAPOrigin = 0,
    #     intGAPPresence = 0,
    #     intGAPReproduction = 0,
    #     intGAPSeason = 0,
    #     eval = 0
    # WHERE weight_sum >= 0 AND intGAPOrigin IS NULL;
    #
    #
    # /*###########################################  Validation column
    # #############################################################*/
    # /*  Populate a validation column.  If an evaluation supports the GAP ranges
    # then it is validated */
    # ALTER TABLE sp_range ADD COLUMN validated_presence INT NOT NULL DEFAULT 0;
    #
    # UPDATE sp_range
    # SET validated_presence = 1
    # WHERE eval = 1;
    #
    #
    # /*#############################################################################
    #                                Export Maps
    #  ############################################################################*/
    # /*  Create a version of sp_range with geometry  */
    # CREATE TABLE new_range AS
    #               SELECT sp_range.*, shucs.geom_5070
    #               FROM sp_range LEFT JOIN shucs ON sp_range.strHUC12RNG = shucs.HUC12RNG;
    #
    # ALTER TABLE new_range ADD COLUMN geom_4326 INTEGER;
    #
    # SELECT RecoverGeometryColumn('new_range', 'geom_5070', 5070, 'MULTIPOLYGON', 'XY');
    #
    # UPDATE new_range SET geom_4326 = Transform(geom_5070, 4326);
    #
    # SELECT RecoverGeometryColumn('new_range', 'geom_4326', 4326, 'POLYGON', 'XY');
    #
    # SELECT ExportSHP('new_range', 'geom_4326', '{2}{1}_CONUS_Range_2020v1',
    #                  'utf-8');
    #
    # /* Make a shapefile of evaluation results */
    # CREATE TABLE eval AS
    #               SELECT strHUC12RNG, eval, geom_4326
    #               FROM new_range
    #               WHERE eval >= 0;
    #
    # SELECT RecoverGeometryColumn('eval', 'geom_4326', 4326, 'MULTIPOLYGON', 'XY');
    #
    # SELECT ExportSHP('eval', 'geom_4326', '{2}{1}_eval', 'utf-8');
    #
    #
    # /*#############################################################################
    #                              Clean Up
    # #############################################################################*/
    # /*  */
    # DROP TABLE intersected_recent;
    # DROP TABLE big_nuff_recent;
    # """.format(eval_id, gap_id, outDir)
    #
    # try:
    #     cursor.executescript(sql2)
    # except Exception as e:
    #     print(e)
    #
    # conn.commit()
    # conn.close()
