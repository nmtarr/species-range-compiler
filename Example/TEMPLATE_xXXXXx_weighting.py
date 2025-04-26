"""
N. Tarr, 5/3/2022

This is a script that applies/adjusts the weights of
records from a wildlife-wrangler output database.

Code can be included to adjust weights in broad strokes based upon attribute
values or on a record by record basis following manual inspection and
assessment of records.
"""
import sqlite3
import sys

wrangler_db = sys.argv[1]
connection = sqlite3.connect(wrangler_db)
cursor = connection.cursor()

def adjust(adjustment, value, where, note):
    """
    Applies a change to weights, appends a notes explaining why

    PARAMETERS
    ----------
    adjustment - whether to increase, decrease, or set the weight value
    value - quantity to adjust the weight by (positive integer)
    where - an optional SQL where clause to pass in order to identify records
        to change
    note - text string documenting the reason for your change
    """
    value = str(value)
    if adjustment == "set":
        sql = """
              UPDATE occurrence_records SET weight = {0} {1};
              UPDATE occurrence_records SET weight_notes = weight_notes || '{2}' {1};
              """.format(value, where, note)
        cursor.executescript(sql)
        connection.commit()

    if adjustment == "decrease":
        sql = """
              UPDATE occurrence_records SET weight = weight - {0} {1};
              UPDATE occurrence_records SET weight_notes = weight_notes || '{2}' {1};
              """.format(value, where, note)
        cursor.executescript(sql)
        connection.commit()

    if adjustment == "increase":
        sql = """
              UPDATE occurrence_records SET weight = weight + {0} {1};
              UPDATE occurrence_records SET weight_notes = weight_notes || '{2}' {1};
              """.format(value, where, note)
        cursor.executescript(sql)
        connection.commit()


#  **************************** START CLEAN ***********************************
cursor.executescript("UPDATE occurrence_records SET weight_notes = '';")
adjust(adjustment="set", value=1, note="", where="")

# **************************** ADJUST DEFAULT *********************************
# Set all records to a standard value as a starting point
default = 5
default_note = """Defualt value of 5 used as starting point and individual
                   record weights then adjusted as necessary."""
adjust(adjustment="set", value=default, note=default_note, where="")

# **************************** BROAD STROKES **********************************
## Change multiple records at the same time for the same reason
adjust(adjustment="set",
       value=9,
       where="WHERE basisOfRecord LIKE '%PRESERVED_SPECIMEN%'",
       note="""Preserved specimens are presumably parts of collections
       curated by knowledgeable people, so misidentification risk should
       be low; """)

for i in ["GEODETIC_DATUM_ASSUMED_WGS84", "GEODETIC_DATUM_INVALID"]:
    adjust(adjustment="decrease", value=3,
           where="WHERE issues LIKE '%{0}%'".format(i),
           note="""Confusion around the geodetic datum reduces certainty
           about the location of the individual recorded; """)

adjust(adjustment="decrease", value=3,
       where="WHERE issues LIKE '%RECORDED_DATE_UNLIKELY%'",
       note=""" Unlikely dates risk unwanted errors;""")


# *************************** ENFORCE LIMITS **********************************
adjust(adjustment="set", value=0, 
       where="WHERE CAST(weight as INTEGER) < 0", note=";")

adjust(adjustment="set", value=10, 
       where="WHERE CAST(weight as INTEGER) > 10", note=";")

connection.close()
print("Finished adjusting weights.")

