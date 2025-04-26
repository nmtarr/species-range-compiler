import sys
#-----------------  Species and other variables  ------------------------------
gap_id = sys.argv[1]  # the GAP species ID
task_id = sys.argv[2]  # the GAP task ID

# Paths to use
workDir = sys.argv[3]
eval_db = workDir + "/" + gap_id + task_id + ".sqlite"

periods = ((2001,2005), (2006,2010), (2011,2015), (2016,2020), (2021,2025))
# ****************************************************************************
import sqlite3
import pandas as pd
from datetime import datetime
print("\nRUNNING TESTS")

start_year = periods[2][0]
end_year = periods[2][1]
connection = sqlite3.connect(eval_db)
cursor = connection.cursor()

# Read in the presence table
df = (pd.read_sql("SELECT * FROM presence;", con=connection)
     # .astype({'strHUC12RNG': 'str',
     #          'presence_2001v2': 'int',
     #          'historical_weight_2005v2': 'int',
     #          'historical_weight_2010v2': 'int',
     #          'historical_weight_2015v2': 'int',
     #          'historical_weight_2020v2': 'int',
     #          'historical_weight_2025v2': 'int',
     #          'recent_weight_2005v2': 'int',
     #          'recent_weight_2010v2': 'int',
     #          'recent_weight_2015v2': 'int',
     #          'recent_weight_2020v2': 'int',
     #          'recent_weight_2025v2': 'int',
     #          })
     .drop('geom_5070', axis=1)
     .fillna(0)
     )

# ------------------------------- TEST 1 -------------------------------------
# If presence was documented before the period, then one of the previous
#    "documented" columns should have a value of 1.  NOTE! Test fails if weight
#   is below in some previous periods but they sum to > 10. 
#  That's not necessarily wrong
# Loop through each period for assessment
# for i in list(range(0, len(periods))):
#     pre_col = "documented_pre" + str(periods[i][0])

#     # List periods before the one being assessed
#     pre_periods = periods[:i]

#     # List necessary "documented" column names
#     doc_cols = ["documented_" + str(x[1]) for x in pre_periods]

#     # Add column to account for pre 2001 records
#     doc_cols.append("documented_pre2001")

#     # Name for sum of previous documented columns
#     past_sum = "past_documented_sum_" + str(periods[i][1])

#     # Name for test column
#     test_col = "past_documented_test_" + str(periods[i][1])

#     # Exclude the first period since it has no previous periods
#     if i != 0:
#         # New column of sum of documented columns
#         df[past_sum] = df[doc_cols].sum(axis=1)

#         # If pre column is 0 ....
#         df1 = df[df[pre_col] == 0].copy()

#         # .... previous documented columns should be zero (sum to 0)
#         df1[test_col] = df1[past_sum] == 0
#         if False in (df1[test_col].unique()):
#             print('FAILED! : {0} "0" conflicts with values in previous documented columns'.format(pre_col))

#         # If pre column is 1....
#         df2 = df[df[pre_col] == 1].copy()

#         # .... then atleast one previous documented column should be 1.
#         df2[test_col] = df2[past_sum] >= 1
#         if False in (df2[test_col].unique()):
#             print('FAILED! : {0} "1" conflicts with values in previous documented columns'.format(pre_col))

# ------------------------------- TEST 2 -------------------------------------
# If presence was documented for a period, then all subsequent periods should
#   indicate past presence, at the least.
for i in list(range(0, len(periods))):
    doc_col = "documented_" + str(periods[i][1])

    # List periods after the one being assessed
    post_periods = periods[i+1:]

    # List necessary "documented_pre" column names
    pre_cols = ["documented_pre" + str(x[0]) for x in post_periods]

    # Name for sum of previous documented columns
    post_sum = "documented_pre_sum_" + str(periods[i][1])

    # Name for test column
    test_col = "later_pre_test_" + str(periods[i][1])

    # Exclude the last period since it has no subsequent periods
    if i != len(periods)-1:
        # New column of sum of documented columns
        df[post_sum] = df[pre_cols].sum(axis=1)

        # If the documented column is 0, there's too much possibility for a test
        # If the documented column is 1....
        df2 = df[df[doc_col] == 1].copy()

        # .... then all subsequent "documented_pre" columns should be "1"
        df2[test_col] = df2[post_sum] == len(pre_cols)
        if False in (df2[test_col].unique()):
            print('FAILED! : {0} "1" conflicts with values in subsequent "documented_pre" columns'.format(doc_col))


# ------------------------------- TEST 3 -------------------------------------
# Historical weight for a given year should be equal to the previous periods
#   "recent" and "historical" weights summed
for i in list(range(0, len(periods))):
    historical_weight = "historical_weight_{0}".format(periods[i][1])

    # List periods before the one being assessed
    pre_periods = periods[:i]

    # List necessary "documented" column names
    weight_cols = ["recent_weight_{0}".format(x[1]) for x in pre_periods]

    # Account for weight from before 2001
    weight_cols.append("historical_weight_2005")

    # Name for sum of previous documented columns
    past_sum = "past_weight_sum_" + str(periods[i][1])

    # Name for test column
    test_col = "past_weight_test_" + str(periods[i][1])

    # Exclude the first period since it has no previous periods
    if i != 0:
        # New column of sum of documented columns
        df[past_sum] = df[weight_cols].sum(axis=1)

        # .... previous documented columns should be zero (sum to 0)
        df[test_col] = df[past_sum] == df[historical_weight]
        if False in (df[test_col].unique()):
            print('FAILED! : {0} conflicts with previous recent columns'.format(historical_weight))

# ------------------------------- TEST 4 -------------------------------------
# Documented columns should have sufficient weight in corresponding columns
for i in list(range(0, len(periods))):
    # Documented column
    doc_col = "documented_" + str(periods[i][1])

    # Weight column
    weight_col = "recent_weight_{0}".format(periods[i][1])

    # Name for test column
    test_col = "weight_test_" + str(periods[i][1])

    # If documented value is 0 ....
    df1 = df[df[doc_col] == 0].copy()

    # .... weight value should be zero (sum to 0)
    df1[test_col] = df1[weight_col] < 10
    if False in (df1[test_col].unique()):
        print('FAILED! : {0} "0" conflicts with weight'.format(doc_col))

    # If pre column is 1....
    df2 = df[df[doc_col] == 1].copy()

    # .... then atleast one previous documented column should be 1.
    df2[test_col] = df2[weight_col] >= 10

    if False in (df2[test_col].unique()):
        print('FAILED! : {0} "1" conflicts with weight'.format(doc_col))


# ------------------------------- TEST 5 -------------------------------------
# Where presence is coded 1 (confirmed) then the corresponding documented
#   column should be "1" as well.
for i in list(range(0, len(periods))):
    # Presence column
    presence_col = "presence_" + str(periods[i][1])

    # Documented column
    doc_col = "documented_" + str(periods[i][1])

    # Documented pre column
    pre_col = "documented_pre" + str(periods[i][0])

    # Name for test column
    test_col = "presence_test_" + str(periods[i][1])

    # List periods before the one being assessed
    pre_periods = periods[:i]

    # Subest rows to test
    df1 = df[df[presence_col] == 1].copy()

    df1[test_col] = df1[presence_col] == df1[doc_col]

    if False in (df1[test_col].unique()):
        print('FAILED! : {0} conflicts with documented'.format(presence_col))

print("FINISHED")
'''
                            OUTDATED TESTS

# ------------------------------- TEST 6 -------------------------------------
# Presence should be 3 where documented pre is 1, but only if presence
#   is not 1.
for i in list(range(0, len(periods))):
    # Presence column
    presence_col = "presence_" + str(periods[i][1]) + "v2"

    # Documented column
    doc_col = "documented_" + str(periods[i][1]) + "v2"

    # Documented pre column
    pre_col = "documented_pre" + str(periods[i][0])

    # Name for test column
    test_col = "presence_test_" + str(periods[i][1])

    # List periods before the one being assessed
    pre_periods = periods[:i]

    # Subest rows to test
    df2 = df[(df[pre_col] == 1) & (df[presence_col] != 1)].copy()

    # Apply test
    df2[test_col] = df2[presence_col] == 3

    # Check results
    if False in (df2[test_col].unique()):
        print('FAILED! : {0} conflicts with presence code'.format(pre_col))

print("Complete -- any errors are listed above")

# ------------------------------ TEST 7 ------------------------------------
# Test documented column against years_since - Can tests be done? it's complex
# One possible logic test: years since can't be older than the documented column,
# but could be younger if a record with insufficient weight existed.
# Identify range compilation year - how to do this?
comp_year = 2022

# Test column
test_col = "age-documented_test"

# Make a dictionary of age range and corresponding column name
period_lookup = {}
for period in periods:
    # Find range of possible ages for period given the compilation year
    age_range = range(comp_year - period[1], (comp_year - period[0]) + 1)
    # Add entry to dictionary
    period_lookup[age_range] = 'documented_{0}v2'.format(period[1])

# Get list of the documented columns
doc_columns = [x for x in df.columns if "documented_20" in x]

# Function to find appropriate column for a given age
def check_yrs(age):
    for p in period_lookup.keys():
        if age in p:
            return period_lookup[x]

# Does years since agree with corresponding documented column
#df[test_col] = [df[check_yrs(x)] == 1 for x in df["yrs_since_record"]]
for x in df["yrs_since_record"]:
    print(x)
    print(df[check_yrs(x)] == 1)

# Find the most recent period with documented
'''
