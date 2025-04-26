"""
Prints the opinions for HUCs selected in QGIS, for the species of interest. 
Assesses presence, summer, winter, and year_round.

N. Tarr, 6/21/2023
"""
species = "mFISHx"

# -----------------------------------------------------------------------------
import pandas as pd
import processing
from datetime import datetime
import numpy as np
import sqlite3

# Make pandas print all columns and cells
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

connection = sqlite3.connect("REPLACETHIS/Vert/DBase/range_opinions.sqlite")

# Read in selected features
selected = iface.activeLayer().selectedFeatures()

hucs0 = []
for s in selected:
    try:
        hucs0.append(s.attribute('strHUC12RNG'))
    except:
        hucs0.append(s.attribute('HUC12RNG'))
if len(hucs0) == 1:
    hucs = str(hucs0[0])
else:
    hucs = str(tuple(hucs0))

# Build a dataframe of opinions for the selected HUCs and species and seasons by 
# querying each table and concatenating the results
df1 = pd.DataFrame()

for s in ["presence", "winter", "summer", "year_round"]:
    # Get the opinions for s
    if len(hucs0) > 1:
        sql = f"""
        SELECT * FROM {s} WHERE strHUC12RNG IN {hucs} AND species_code = '{species}';
        """
    if len(hucs0) == 1:
        sql = f"""
        SELECT * FROM {s} WHERE strHUC12RNG = '{hucs}' AND species_code = '{species}';
        """
    df = pd.read_sql(sql, connection)

    # Add a column for the season
    df["season"] = s

    # Concatenate the results
    df1 = pd.concat([df, df1])

# Drop the index
df1.reset_index(drop=True, inplace=True)

# Print
print(df1)