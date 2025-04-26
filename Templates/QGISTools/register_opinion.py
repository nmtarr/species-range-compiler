""" Adds opinions to range_opinions.sqlite """
#----------------------- EDIT ACCORDINGLY -------------------------------------
species_code = "mAMMAx"
season = "year_round" # presence, summer, year_round, or winter
start_year = 2005
end_year = 2005 # do not exceed current year
status = "present" #present or absent 
justification = """
This unit is very close to and connected to ones where the species was detected 
or itself included detections before 2005. 
""".replace("\n", "")
type = "tertiary"
references = ""
confidence = 6
my_initials = "NMT"
expert_rank = 6

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

# Build an empty data frame of selected features
DF = pd.DataFrame(columns=[],
                  index=hucs)

# Fill out the data frame
DF["species_code"] = species_code
DF["status"] = status
DF["confidence"] = confidence
DF["year"] = start_year
DF["justification"] = justification
DF["type"] = type
DF["citations"] = references
DF["expert"] = my_initials
DF["expert_rank"] = expert_rank
DF["entry_time"] = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")

# Move the index to a column
DF.index.name="strHUC12RNG"
DF.reset_index(drop=False, inplace=True)
DF.index.name="id"

DF1 = DF.copy()

# Expand dataframe (add rows) to include all years
for i in np.arange(start_year, end_year+1)[1:]:
    DF2 = DF.copy()
    DF2["year"] = i
    DF1 = pd.concat([DF1, DF2])

# Write to database
DF1.to_sql(name=season, if_exists="append", con=connection, index=False)

# Deselect
mc = iface.mapCanvas()

for layer in mc.layers():
    if layer.type() == layer.VectorLayer:
        layer.removeSelection()

mc.refresh()

# Done
print("Opinions were saved.")
