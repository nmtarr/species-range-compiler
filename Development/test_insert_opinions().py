"""
N. Tarr, October 13, 2022.

Tests the code used for reconciling and cleaning up expert opinion records.
Utilizes a test dataset (table) for which answers are knowable according
to the specified rules of reconciling conflicting opinions.  Copy, paste,
and adapt the code from insert_opinions() to pull from the test table in
range_opinions.sqlite.

Successful code will use yield "pass" from the tests.
"""
import pandas as pd
import sqlite3

opinion_db = "REPLACETHIS/Vert/DBase/range_opinions.sqlite"
species = "TEST1"
years = (2000, 2001)


# Retrieve the data -----------------------------------------------------------
connection = sqlite3.connect(opinion_db)
df1 = (pd.read_sql("""SELECT * FROM test
                      WHERE species_code = :species;""",
                  connection,
                  params={"species": species}
                  )
       [lambda x: x["year"].isin(years)==True]
       )


# Drop duplicates -------------------------------------------------------------
"""Records that are identical to another record should be dropped"""
df2 = df1.drop_duplicates()


# Drop older entries from each expert -----------------------------------------
"""If an expert entered records for the same unit at different times,
keep the more recent record and drop the other"""

# Set columns to group on
df3 = (df2.sort_values(by="entry_time", ascending=False)
       .groupby(['strHUC12RNG', 'year', 'species_code', 'expert'])
       .first()
       .reset_index()
       )


# Drop negated opinions -------------------------------------------------------
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


# Drop lower expert rank or confidence ----------------------------------------
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


# Drop records for older years ------------------------------------------------
"""Opinions are assessed within the periods of years for the range map, and
conflicts could occur between, for example, the first year of the period and
the last.  The rule for this is to go with the record for the latest year.
"""
df8 = (df5.sort_values(by=["year"], ascending=False)
       .groupby(['strHUC12RNG'])
       .first()
       .reset_index()
       )

# Tests -----------------------------------------------------------------------
# TEST 1
if len(df1) == (len(df2) + 1):
    print("Test 1: pass")
else:
    print("Test 1: fail")

# TEST 2
df6 = df5[df5["strHUC12RNG"].isin(["111", "222", "444", "666"]) == True]
if df6["fate"].unique() == "win":
    print("Test 2: pass")
else:
    print("Test 2: fail")

# TEST 3
if "omit" not in df5["fate"]:
    print("Test 3: pass")
else:
    print("Test 3: fail")

# TEST 4
if df8["year"].unique() == 2001.0:
    print("Test 4: pass")
else:
    print("Test 4: fail")


"""
x2 = (df2
      .sort_values(by="entry_time", ascending=False)
      .groupby(cols1)
      .count()
      [lambda x: x["status"] == 2]
      )

df = (df
      .sort_values(by=['decimalLatitude', 'decimalLongitude', 'eventDate',
                       'radius_m'],
                   ascending=True, kind='mergesort', na_position='last')
      .drop_duplicates(subset=['decimalLatitude', 'decimalLongitude',
                               'eventDate'],
                       keep='first'))
"""

"""
df = pd.DataFrame({"rank": [random.randint(1,10) for i in range(1000)],
                   "confidence": [random.randint(1,10) for i in range(1000)]})
df["score"] = df["rank"]*(df["confidence"]/10.)
print(df)
df.plot(x="confidence", y="rank", kind="scatter")
plt.show()
"""
