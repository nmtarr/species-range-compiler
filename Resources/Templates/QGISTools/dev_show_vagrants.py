"""
Selects and displays spatial units that are deemed extra-limital.

This script attempts to use python resources, instead of spatialite.

N. Tarr, 2/13/2023
"""
"""**************************************************************************"""
import sqlite3
from PyQt5.QtGui import *
from random import randrange
import numpy as np
import pandas as pd

#-------------------  Species and version variables  --------------------------
task_name = "NePrRu" # a short, memorable name to use for file names etc
gap_id = "bGCWAx"
task_id = "new-pres-rules"
version = "v2"

#------------------------------  Paths to use  --------------------------------
# List occurrence record databases in order of precendence.  Records from the
# first will take presedence if duplicates arise.
ww_output = ("REPLACETHIS/Workspaces/RangeMaps/new-pres-rules/NePrRu.sqlite",
             )
outDir = "REPLACETHIS/Workspaces/RangeMaps/new-pres-rules/"
codeDir = "REPLACETHIS/Code/GAP-ranges/"
tmpDir = "REPLACETHIS/Workspaces/RangeMaps/new-pres-rules/"
parameters_db = "REPLACETHIS/Vert/DBase/range-parameters.sqlite"
grid_db = "REPLACETHIS/Datasets/huc12rng_gap_polygon.sqlite"
wrangler_path = "REPLACETHIS/Code/wildlife-wrangler/"
task_db = outDir + gap_id + task_id + ".sqlite"
periods = ((2001,2005), (2006,2010), (2011,2015), (2016,2020), (2021,2025))

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
    
cursor, connection = spatialite(task_db)

# Get the documuented hucs in a list -------------------------------------------
sql = """SELECT strHUC12RNG FROM presence 
         WHERE extralimital_2020 = 1;"""
extral = cursor.execute(sql).fetchall()
extral = [x[0] for x in extral]
print(extral)

# Select the documented hucs
layer = iface.activeLayer()
for x in extral:
    layer.selectByExpression('"strHUC12RNG" = {0}'.format(x), 
                              QgsVectorLayer.AddToSelection)