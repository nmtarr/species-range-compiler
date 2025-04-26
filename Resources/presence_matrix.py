"""
N. Tarr
11/7/22

This code used for developing rules for handling cases of disagreement among
information sources (opinion, occurrence data, and previous presence codes).
The method used was to be build dataframes of all possible combinations of
codes and then fill out a presence column according to clearly specified rules.
The results can be tested and referenced for understanding methods.

Sections
1. A dataframe that specifies rules of dominance for various information
sources.
2. A dataframe with all types of information combinations and presence-coding
rules applied.  2015 is used as an example year.


Notes
* Rules are applied here to a single time period (2015) but in the range
compiler script, multiple periods are assessed.

* Rules are applied in python here for clarity, but SQL is used in the range
compiler for speed.

"""
import pandas as pd
pd.options.display.max_rows = 100
pd.options.display.max_columns = 10
pd.options.display.width = 200
import random
import itertools
import matplotlib.pyplot as plt

# -----------------------------------------------------------------------------
# ----------------- 1. Information source hierarchy ---------------------------
# -----------------------------------------------------------------------------
# Make a table of pairwise comparisons
sources = ["opinion", "documented", "v1", "previous"]

vs = []
for p in list(itertools.permutations(sources,2)):
    p = list(p)
    p.sort()
    if p not in vs:
        vs.append(p)
df3 = pd.DataFrame(vs, columns=['source1', 'source2'])
df3["dominant"] = pd.NA

# Identify winners
# Documented occurrence always wins
df3.loc[df3["source1"] == 'documented', "dominant"] = "documented"

# Version 1 always loses
df3.loc[df3["source2"] == 'v1', "dominant"] = df3["source1"]

# Previous vs opinion depends on opinion score (rank*(confidence/10))
df3.loc[(df3["source1"] == "opinion") &
          (df3["source2"] == "previous"),
          "dominant"] = "opinion where rank*(confidence/10) > 2"

print(df3)


# -----------------------------------------------------------------------------
# ---------------------------- 2. Scoring -------------------------------------
# -----------------------------------------------------------------------------
# Opinion has to be weighted/scored when there is a previous presence code
# available in addition to opinion.  This section shows the relationship
# the score, expert rank, and confidence.
# Make a dataframe of scores for pairwise rank-confidence values
v = [1,2,3,4,5,6,7,8,9,10]
v.reverse()
df5 = pd.DataFrame(columns=v, index=v)
for i in df5.index:
    for c in df5.columns:
        df5.loc[i, c] = i*c/10
df5.index.name = "Confidence"
df5.columns.name = "Expert Rank"
print(df5)

# Make a dataframe that shows which cases opinion would get used (1) absent
# a previous presence code.
df6 = df5 > 2
df6.replace({True: 1, False: 0}, inplace=True)
print(df6)


# -----------------------------------------------------------------------------
# ------------------------ 3. Rule specification ------------------------------
# -----------------------------------------------------------------------------
# Possible values
documented = [1,pd.NA]
last_period = [1,2,3,4,5,pd.NA]
status = [0,1,pd.NA]
GAP2001 = [1,2,3,4,5,6,7,pd.NA]
opinion_score = [2,9]
confidence = list(range(1,11,1))
rank = list(range(1,11,1))

# Make a table with all combinations, use 2015 as an example.
# Opinion score is rank*(confidence/10), but only 2 and 9 are used here to
# reduce table size. 2 would be subordinate to a past code, 9 would not be.
df1 = pd.DataFrame(columns=["presence_2015v2", "documented_2015v2",
                            "opinion_2015", "opinion_score",
                            "presence_2010v2", "presence_2001v1", "notes"])

for doc in documented:
    for sta in status:
        for las in last_period:
            for gap in GAP2001:
                for opi in opinion_score:
                    new = {"documented_2015v2" : doc,
                           "opinion_2015" : sta,
                           "opinion_score" : opi,
                           "presence_2010v2" : las,
                           "presence_2001v1" : gap}
                    df1 = df1.append(new, ignore_index = True)

# Some values aren't in the 2001v1 maps, remove those
df1 = df1[df1["presence_2001v1"].isin([2,3,5,6,7]) == False]


# --------------------------------- Populate presence values according to rules
# Put rules into a function here
def rules(df):
    # ---------------------------- 2001v1 -------------------------------------
    # If a 2001v1 code exists, use that as a start
    df["presence_2015v2"] = df["presence_2001v1"]

    # Old legend values 1,2,3 become new legend value 3
    df.loc[df["presence_2015v2"].isin([1,2,3]) == True, 'presence_2015v2'] = 3

    # Old legend values 4,5 become new legend value 4
    df.loc[df["presence_2015v2"].isin([4,5]) == True, 'presence_2015v2'] = 4


    # --------------------- Previous Period Code ------------------------------
    # If documented in previous time step, code as 3
    df.loc[df["presence_2010v2"] == 1, 'presence_2015v2'] = 3

    # If coded as present in previous time step, code as 3
    df.loc[df["presence_2010v2"].isin([2,3]) == True, 'presence_2015v2'] = 3

    # If coded as suspected absent in previous time step, code as 4
    df.loc[df["presence_2010v2"].isin([4,]) == True, 'presence_2015v2'] = 4

    # If coded as likely absent in previous time step, code as 5
    df.loc[df["presence_2010v2"].isin([5,]) == True, 'presence_2015v2'] = 5


    # --------------------------- Opinion -------------------------------------
    # If opinion with a high enough score exists, use it to overwrite null
    # values and codes from previous periods (including 2001v1)
    # Believed present
    df.loc[(df["opinion_score"] > 2) &
            (df["opinion_2015"] == 1), 'presence_2015v2'] = 3

    # Believed absent
    df.loc[(df["opinion_score"] > 2) &
            (df["opinion_2015"] == 0), 'presence_2015v2'] = 4


    # ------------------------ Occurrence Records -----------------------------
    # If documented with records, presence is documented
    df.loc[df["documented_2015v2"] == 1, 'presence_2015v2'] = 1

    # Remove cases where all columns are null
    df.dropna(how='all', inplace=True)

    return df

# Apply rules to fill out presence column
df2 = rules(df1)

# Remove cases of an opinion score but all other values as null, which is just
# an artifact of table creation.
df2 = df2[df2["opinion_2015"].isnull() == False]

# Remove cases where everything but opinion is null, and opinion score is too low.
df2 = df2[(df2["opinion_score"] > 3) &
          (df2["presence_2015v2"].isnull() == False)]

# Save to file
df2.to_csv("T:/RangeMaps/presence_coding_matrix.csv")


# ----------------------------------------------------------------------- Tests
# 1. There shouldn't be any cases with null presence values for the period
nulls = df2[df2["presence_2015v2"].isnull() == True]
if len(nulls) == 0:
    print('Test 1: pass')
else:
    print(nulls)

# 2. All of the potential codes for each source should still be present
r = 0
def check_values(column, OK_values, r = r, df = df2):
    '''Gets unique values from a column, excludes nan'''
    vals = list(df[column].unique())
                                                                               #vals = vals[~np.isnan(vals)]
    # Are all column values OK?
    violations = set(vals) - set(OK_values)
    if len(violations) != 0:
        print("Test 2: failed on {0}".format(column))
        print("\t violations: " + str(violations))
        r = 1
    # Are all values represented?
    missing = set(OK_values) - set(vals)
    if len(missing) != 0:
            print("Test 2: failed on {0}".format(column))
            print("\t extras: " + str(missing))
            r = 1
    return r

# Presence 2015 values should be 1, 2, 3, 4, or 5
r = check_values("presence_2015v2", last_period)

# Presence 2010 values should be 1, 2, 3, 4, or 5
r = check_values("presence_2010v2", last_period)

# Documented values should be 1 or nan
r = check_values("documented_2015v2", documented)

# Opinion values should be 1, 0, or nan
r = check_values("opinion_2015", status)

# Opinion score should be 2 or 9
r = check_values("opinion_score", [2,9])

# 2001v1 values should be 3 or 4 (or null)
r = check_values("presence_2001v1", [1.0, 4.0])

# Report a pass of test 2
if r == 0:
    print("Test 2: pass")

# 3. If documented present, then presence code should be 1
df7 = df2[df2["documented_2015v2"] == 1]
df8 = df7["presence_2015v2"] == df7["documented_2015v2"]
if bool(df8.unique()) == True:
    print("Test 3: pass")
else:
    print("Test 3: failed!!!!!!!!")


# 4. All codes for each source should be present in final table.
t4a = df2[(df2["presence_2015v2"].isnull() == False)
          #& (df2["documented_2015v2"].isnull() == True)
          #& (df2["opinion_2015"].isnull() == False)
          & (df2["opinion_score"] == 2.)
          #& (df2["presence_2010v2"].isna() == True)
          #& (df2["presence_2010v2"].isna() == False)
         ]
print(t4a)


# ----------------------- END END END END END ---------------------------------
"""
# Opinion-previous conflicts --------------------
df4 = pd.DataFrame(columns=["presence_2015v2", "presence_2010v2",
                            "opinion_2015", "opinion_score", "notes"])

for sta in [x for x in status if pd.notnull(x)]:
    for las in [x for x in last_period if pd.notnull(x)]:
        for sco in [10,8,6,4,2,1]:
            new = {"opinion_2015" : sta,
                   "presence_2010v2" : las,
                   "opinion_score" : sco}
            df4 = df4.append(new, ignore_index = True)

# Remove cases where all columns are null
df4.dropna(how='all', inplace=True)

# Use opinion unless score is below 3
df4.loc[(df4["opinion_score"] > 2) &
        (df4["opinion_2015"] == 1), 'presence_2015v2'] = 3

df4.loc[(df4["opinion_score"] > 2) &
        (df4["opinion_2015"] == 0), 'presence_2015v2'] = 4

print(df4)
"""
