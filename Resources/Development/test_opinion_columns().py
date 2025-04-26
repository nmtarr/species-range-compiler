"""
Tests the processing code for creating columns with the most recent opinion
that passed through the reconciliation of all opinions.
"""
import pandas as pd
import sqlite3
from datetime import datetime

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

time1 = datetime.now()

species = "bWPWIx"
task_db = "REPLACETHIS/Workspaces/RangeMaps/WhipPoorWill/bWPWIxCompilePresV2.sqlite"
start_year = 2016
end_year = 2021

cursor, conn = spatialite(task_db)

# --------------------------------------------------- Put opinion into a column
def opinion_column(start_year, end_year, conn, cursor):
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
    sql="""
        SELECT load_extension("mod_spatialite");
        ALTER TABLE presence ADD COLUMN opinion_{1} TEXT;

        /* Insert rows into presence for HUCs that have an opinion but are
           not yet included in presence. */
        INSERT INTO presence (strHUC12RNG)
            SELECT DISTINCT O.hucs
            FROM (SELECT DISTINCT opinions.strHUC12RNG AS hucs FROM opinions) AS O
                  LEFT JOIN presence ON presence.strHUC12RNG = O.hucs
            WHERE presence.strHUC12RNG IS NULL;

        UPDATE presence
        SET opinion_{1} = B.status
        FROM (SELECT MAX(ROWID), strHUC12RNG, status
              FROM opinions
        	  WHERE year BETWEEN {0} AND {1}
        	  GROUP BY strHUC12RNG
        	  ORDER BY year DESC)
              AS B
        WHERE presence.strHUC12RNG = B.strHUC12RNG;

        UPDATE presence
        SET opinion_{1} = 0
        WHERE opinion_{1} = "absent";
        """.format(str(start_year), str(end_year))
    try:
        cursor.executescript(sql)
        conn.commit()
        print('Added column for most recent opinion between ({0}-{1}): '.format(str(start_year), str(end_year)) + str(datetime.now() - time1))
    except Exception as e:
        print(e)
        print("!!!!!!", end_year)

# Run
opinion_column(start_year, end_year, conn, cursor)

# TESTS -----
# Every record with an opinion in the opinion table should have a non-null
#   opinion value.

# Get a list of hucs represented in the opinions table
sql = """SELECT DISTINCT strHUC12RNG
         FROM opinions
         WHERE year BETWEEN {0} AND {1};
      """.format(str(start_year), str(end_year))

df1 = pd.read_sql(sql, con=conn)

# Get a list of hucs with a non-null value from presence
sql = """SELECT strHUC12RNG
         FROM presence
         WHERE opinion_{0} IS NOT NULL;
      """.format(str(end_year))
df2 = pd.read_sql(sql, con=conn)

# Test for equality of values
print(set(df1['strHUC12RNG']) == set(df2['strHUC12RNG']))

#tuple(set(df1['strHUC12RNG']) - set(df2['strHUC12RNG']))


# PANDAS ATTEMPT
    # # Drop records for older years ------------------------------------------
    # """Opinions are assessed within the periods of years for the range map, and
    # conflicts could occur between, for example, the first year of the period and
    # the last.  The rule for this is to go with the record for the latest year.
    # """
    # df8 = (df5.sort_values(by=["year"], ascending=False)
    #        .groupby(['strHUC12RNG'])
    #        .first()
    #        .reset_index()
    #        )
    # Write to opinions table
