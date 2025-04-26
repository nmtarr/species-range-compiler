""" Deletes opinions from range_opinions.sqlite """
#----------------------- EDIT ACCORDINGLY -------------------------------------
species_code = "mEFSQx"
season = "presence" # presence, summer, year_round, or winter
start_year = 2001
end_year = 2002  # do not exceed current year
status = "absent" #present or absent 
type = "tertiary"
references = ""
my_initials = "NMT"

#-------------------------- DO NOT CHANGE BELOW HERE --------------------------
import pandas as pd
import processing
from datetime import datetime
import numpy as np
import sqlite3

connection = sqlite3.connect("REPLACETHIS/Vert/DBase/range_opinions.sqlite")

# Read in selected features
selected = iface.activeLayer().selectedFeatures()

hucs = []
for s in selected:
    try:
        hucs.append(s.attribute('strHUC12RNG'))
    except:
        hucs.append(s.attribute('HUC12RNG'))
hucs = tuple(hucs)

if len(hucs) == 1:
    hucs = tuple(hucs[0])

# Make a tuple of years
years = tuple(np.arange(start_year, end_year+1))
if len(years) == 1:
    hucs = tuple(years[0])

# Build delete query
delete_query = f"""
DELETE FROM {season}
WHERE species_code = "{species_code}"
AND year IN {years}
AND status = "{status}"
AND type = "{type}"
AND expert = "{my_initials}"
AND citations = "{references}"
AND strHUC12RNG IN {hucs};
"""

# Send a delete query to the database
cursor = connection.cursor()
cursor.execute(delete_query)
connection.commit()

# Deselect
mc = iface.mapCanvas()

for layer in mc.layers():
    if layer.type() == layer.VectorLayer:
        layer.removeSelection()

mc.refresh()

# Done
print("OPINIONS WERE DELETED.")