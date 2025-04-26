"""
N. Tarr
June 10, 2022

Computes a summary of changes in various aspects of the ranges over time
periods.  Summaries are based upon the presence and occurrence records
tables from a GAP-range output database.  A new table is created in the
output database with the name "change_summary".
"""

import sqlite3
import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt

# Arguments -------------------------------------------------------------------
sp = sys.argv[1]  # GAP species code
task_name = sys.argv[2]  # a short, memorable name to use for file names etc
workDir = sys.argv[3]
rangeDB = workDir + "/" + sp + task_name + ".sqlite"
output_csv = sys.argv[4]
# -----------------------------------------------------------------------------

# Define periods
periods = ((2001,2005), (2006,2010), (2011,2015), (2016,2020), (2021,2025))

# Connect to the database
connection = sqlite3.connect(rangeDB)
cursor = connection.cursor()


# Read in presence table ------------------------------------------------------
pres_df0 = pd.read_sql(sql="SELECT * FROM presence;", con=connection)


# Count of presence per year --------------------------------------------------
pres_df1 = (pres_df0[["presence_{0}".format(str(x[1])) for x in periods]]
            .copy()
            .fillna(5)
            .astype(int)
            .set_axis([x[1] for x in periods], axis=1)
            )

# Change pres_df1 values accordingly.  Hint: read where statement as
# "keep dataframe the same where condition else change to value"
pres_df2 = pres_df1.where((pres_df1 < 4), 0)
pres_df2.where(pres_df2 == 0, 1, inplace=True)

# Convert back to data frame and sum columns
pres_df2 = pd.DataFrame(pres_df2.sum())
pres_df2.columns = ["present(n)"]


# Count of documented present table -------------------------------------------
doc_df0 = (pres_df0[["documented_{0}".format(str(x[1])) for x in periods]]
           .copy()
           .set_axis([x[1] for x in periods], axis=1)
          )

# Convert back to data frame and sum columns
doc_df1 = pd.DataFrame(doc_df0.sum()).astype(int)
doc_df1.columns = ["documented(n)"]

# Merge with previous table
summary_df = pres_df2.merge(doc_df1, left_index=True, right_index=True)


# Proportion of present that are documented -----------------------------------
pres_df10 = pres_df2.merge(doc_df1, left_index=True, right_index=True).astype(int)
pres_df10["documented(prop_present)"] = (pres_df10["documented(n)"]/pres_df10["present(n)"])
pres_df10 = pres_df10[["documented(prop_present)"]]

# Merge with previous table
summary_df = summary_df.merge(pres_df10, left_index=True, right_index=True)


# Previously documented in a previous period ----------------------------------
docp_df0 = pres_df0[["documented_pre{0}".format(str(x[0])) for x in periods]]
docp_df0.columns = [x[1] for x in periods]
docp_df1 = pd.DataFrame(docp_df0.sum()).astype(int)
docp_df1.columns = ["previously_documented(n)"]

# Merge with previous table
summary_df = summary_df.merge(docp_df1, left_index=True, right_index=True)


# Proportion of CONUS coded present and absent --------------------------------
CONUS_HUCs = 82717
pres_df13 = (pres_df2/CONUS_HUCs)
pres_df13.columns = ["present(prop_CONUS)"]
pres_df13["absent(prop_CONUS)"] = 1 - pres_df13["present(prop_CONUS)"]

# Merge with previous table
summary_df = summary_df.merge(pres_df13, left_index=True, right_index=True)


# Proportion of CONUS documented ----------------------------------------------
doc_df2 = (doc_df1/CONUS_HUCs)
doc_df2.columns = ["documented(prop_CONUS)"]

# Merge with previous table
summary_df = summary_df.merge(doc_df2, left_index=True, right_index=True)


# Read in occurrence records --------------------------------------------------
obs_df0 = (pd.read_sql(sql="SELECT record_id, strftime('%Y', eventDate) AS year FROM occurrence_records;", con=connection)
           .astype({"year": int})
           )


# Records per period ----------------------------------------------------------
# Define a year category object
cats = pd.cut(obs_df0["year"],
              [periods[0][0], periods[0][1], periods[1][1], periods[2][1],
               periods[3][1], periods[4][1]],
             labels=["2005", "2010", "2015", "2020", "2025"])

# Tally
obs_count = pd.value_counts(cats).sort_index()
obs_count = pd.DataFrame(obs_count)
obs_count.index = summary_df.index
obs_count.columns = ["observations(n)"]

# Merge with previous table
summary_df = summary_df.merge(obs_count, left_index=True, right_index=True)

# Records vs documented -------------------------------------------------------
summary_df["observations/documented"] = summary_df["observations(n)"]/summary_df["documented(n)"]

# Records vs present ----------------------------------------------------------
summary_df["observations/documented"] = summary_df["observations(n)"]/summary_df["documented(n)"]


# Mean observation weight -----------------------------------------------------
obs_df0 = (pd.read_sql(sql="SELECT strftime('%Y', eventDate) AS year, weight FROM occurrence_records;",
                       con=connection)
           .astype({"weight": int, "year": int})
           )

weight_mean = pd.DataFrame(obs_df0.groupby(cats).mean()["weight"])
weight_mean.columns = ["mean_weight"]
weight_mean.index = summary_df.index

# Merge with previous table
summary_df = summary_df.merge(weight_mean, left_index=True, right_index=True)


# Percent change in present ---------------------------------------------------
# Calculate a percent change column
pres_df2["change_in_present(%)"] = 0

for i in range(1, len(pres_df2.index)):
    x = ((pres_df2.iloc[i]["present(n)"] - pres_df2.iloc[i-1]["present(n)"])/pres_df2.iloc[i-1]["present(n)"])*100
    pres_df2.loc[pres_df2.index[i], "change_in_present(%)"] = x

# Clean up and round values
pres_df3 = pres_df2[["change_in_present(%)"]]

# Merge with previous table
summary_df = summary_df.merge(pres_df3, left_index=True, right_index=True)


# Change in absent ------------------------------------------------------------
# Make an absence data frame by subtraction
abs_df1 = CONUS_HUCs - pres_df2[["present(n)"]]
abs_df1.columns = ["absent(n)"]

# Calculate a percent change column
abs_df1["change_in_absent(%)"] = 0

for i in range(1, len(abs_df1.index)):
    x = ((abs_df1.iloc[i]["absent(n)"] - abs_df1.iloc[i-1]["absent(n)"])/abs_df1.iloc[i-1]["absent(n)"])*100
    abs_df1.loc[abs_df1.index[i], "change_in_absent(%)"] = x


# Merge with previous table
summary_df = summary_df.merge(abs_df1, left_index=True, right_index=True)


# Touch up main summary table -------------------------------------------------
summary_df = (summary_df[["present(n)", "documented(n)", "absent(n)",
                          "present(prop_CONUS)", "documented(prop_CONUS)",
                          "absent(prop_CONUS)", "documented(prop_present)",
                          "previously_documented(n)", "change_in_present(%)",
                          "change_in_absent(%)", "observations(n)",
                          "mean_weight", "observations/documented"]]
              .astype({"present(n)": 'int32', "absent(n)": 'int32',
                       "documented(n)": 'int32',
                       "previously_documented(n)": 'int32',
                       "observations(n)": 'int32'})
             )
print(summary_df.T)

# Write to database -----------------------------------------------------------
summary_df.to_sql("change_summary", con=connection, if_exists='replace',
                  index_label="period")
connection.commit()
connection.close()
del cursor

if output_csv == True:
    summary_df.to_csv(workDir + sp + "change_summary.csv", header=True)


# Make a figure ---------------------------------------------------------------
# define colors to use
colConf = '#2ca25f' #'#634b67'
colPres1 = '#99d8c9' #'#89688f' 
colAbs = '#636363' #'#b08e28'

# define subplots
fig, axes  = plt.subplots(nrows=3, ncols=2, figsize=(8,8), sharex=True)

# Add plot
df = (summary_df[["documented(n)",
                 "previously_documented(n)"]]
      .rename({"documented(n)": "documented",
                 "previously_documented(n)": "previously documented"},
             axis=1))

df.plot(ax=axes[1,1],
        ylabel="spatial units",
        xlabel="period",
        xticks=[x[1] for x in periods],
        title="",
        color=[colConf, colConf],
        style=['-', '--'])

# Add plot
df = (summary_df[["documented(prop_CONUS)", "documented(prop_present)"]]
      .rename({"documented(prop_CONUS)": "documented/CONUS",
               "documented(prop_present)": "documented/present"},
              axis=1))

df.plot(ax=axes[1,0],
        ylabel="proportion",
        xlabel="period",
        xticks=[x[1] for x in periods],
        #ylim=(0,1),
        title="",
        color=[colConf, colConf],
        style=[':', '--'])

# Add plot
df = (summary_df[["present(prop_CONUS)", "absent(prop_CONUS)"]]
      .rename({"present(prop_CONUS)": "present",
               "absent(prop_CONUS)": "absent"},
             axis=1))
df.plot(ax=axes[0,0],
        ylabel="proportion of CONUS",
        xlabel="period",
        xticks=[x[1] for x in periods],
        ylim=(0,1),
        title="",
        color=[colPres1, colAbs],
        style=['-', '-'])

# Add plot
df = (summary_df[["change_in_present(%)", "change_in_absent(%)"]]
      .rename({"change_in_present(%)": "present",
               "change_in_absent(%)": "absent"}, axis=1))
df.plot(ax=axes[0,1],
        ylabel="% change",
        xlabel="period",
        xticks=[x[1] for x in periods],
        #ylim=(0,100),
        title="",
        color=[colPres1, colAbs],
        style=['-', '-'])

# Number and weight of observation records
summary_df[["observations(n)"]].plot(ax=axes[2,0],
                                     ylabel="observation records",
                                     xlabel="period",
                                     xticks=[x[1] for x in periods],
                                     title="",
                                     color="#dc26d0",
                                     legend=False)

summary_df[["mean_weight"]].plot(ax=axes[2,0],
                                 #ylabel="mean weight",
                                 #ylim=(0,10),
                                 color="#dc26d0",
                                 style=[':'],
                                 legend=False,
                                 secondary_y=True)

axes[2,0].right_ax.set_ylabel("mean weight")
axes[2,0].right_ax.set_ylim(0,10)

# Number of observation per documented
summary_df[["observations/documented"]].plot(ax=axes[2,1],
                                     ylabel="observations/documented",
                                     xlabel="period",
                                     xticks=[x[1] for x in periods],
                                     title="",
                                     color="black",
                                     legend=False)

# Add title and adjust spacing
plt.suptitle("Change in {0} Presence".format(sp))
plt.subplots_adjust(hspace=.1, wspace=.6)

# Save
plt.savefig(workDir + "/" + sp + "_presence_change.jpg", dpi=300)
