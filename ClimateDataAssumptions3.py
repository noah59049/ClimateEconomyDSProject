import numpy as np
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
import time
import gc

# Assumptions:

# ChatGPT reads the data from files correctly

#DONE Every dataframe has the same columns

#DONE Every measurement column has the comp_ and meas_ and years_ flag columns associated with it

#DONE comp_flag_ is always 'S' or ' '

# measurement value is -9999 iff comp_flag_ is ' ';

# who knows about the years, there will probably be some assumptions

def get_ith_dataframe(i):
    return pd.read_csv(f"{i}.csv")

NUM_LINKS = 467

# LOL, EXPECTED_COLUMNS is 2700 characters long for 1 line
# Who needs PEP8? Not me
EXPECTED_COLUMNS = ['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'DATE', 'month', 'day', 'hour', 'HLY-TEMP-NORMAL', 'meas_flag_HLY-TEMP-NORMAL', 'comp_flag_HLY-TEMP-NORMAL', 'years_HLY-TEMP-NORMAL', 'HLY-TEMP-10PCTL', 'meas_flag_HLY-TEMP-10PCTL', 'comp_flag_HLY-TEMP-10PCTL', 'years_HLY-TEMP-10PCTL', 'HLY-TEMP-90PCTL', 'meas_flag_HLY-TEMP-90PCTL', 'comp_flag_HLY-TEMP-90PCTL', 'years_HLY-TEMP-90PCTL', 'HLY-DEWP-NORMAL', 'meas_flag_HLY-DEWP-NORMAL', 'comp_flag_HLY-DEWP-NORMAL', 'years_HLY-DEWP-NORMAL', 'HLY-DEWP-10PCTL', 'meas_flag_HLY-DEWP-10PCTL', 'comp_flag_HLY-DEWP-10PCTL', 'years_HLY-DEWP-10PCTL', 'HLY-DEWP-90PCTL', 'meas_flag_HLY-DEWP-90PCTL', 'comp_flag_HLY-DEWP-90PCTL', 'years_HLY-DEWP-90PCTL', 'HLY-PRES-NORMAL', 'meas_flag_HLY-PRES-NORMAL', 'comp_flag_HLY-PRES-NORMAL', 'years_HLY-PRES-NORMAL', 'HLY-PRES-10PCTL', 'meas_flag_HLY-PRES-10PCTL', 'comp_flag_HLY-PRES-10PCTL', 'years_HLY-PRES-10PCTL', 'HLY-PRES-90PCTL', 'meas_flag_HLY-PRES-90PCTL', 'comp_flag_HLY-PRES-90PCTL', 'years_HLY-PRES-90PCTL', 'HLY-CLDH-NORMAL', 'meas_flag_HLY-CLDH-NORMAL', 'comp_flag_HLY-CLDH-NORMAL', 'years_HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'meas_flag_HLY-HTDH-NORMAL', 'comp_flag_HLY-HTDH-NORMAL', 'years_HLY-HTDH-NORMAL', 'HLY-CLOD-PCTCLR', 'meas_flag_HLY-CLOD-PCTCLR', 'comp_flag_HLY-CLOD-PCTCLR', 'years_HLY-CLOD-PCTCLR', 'HLY-CLOD-PCTFEW', 'meas_flag_HLY-CLOD-PCTFEW', 'comp_flag_HLY-CLOD-PCTFEW', 'years_HLY-CLOD-PCTFEW', 'HLY-CLOD-PCTSCT', 'meas_flag_HLY-CLOD-PCTSCT', 'comp_flag_HLY-CLOD-PCTSCT', 'years_HLY-CLOD-PCTSCT', 'HLY-CLOD-PCTBKN', 'meas_flag_HLY-CLOD-PCTBKN', 'comp_flag_HLY-CLOD-PCTBKN', 'years_HLY-CLOD-PCTBKN', 'HLY-CLOD-PCTOVC', 'meas_flag_HLY-CLOD-PCTOVC', 'comp_flag_HLY-CLOD-PCTOVC', 'years_HLY-CLOD-PCTOVC', 'HLY-HIDX-NORMAL', 'meas_flag_HLY-HIDX-NORMAL', 'comp_flag_HLY-HIDX-NORMAL', 'years_HLY-HIDX-NORMAL', 'HLY-WCHL-NORMAL', 'meas_flag_HLY-WCHL-NORMAL', 'comp_flag_HLY-WCHL-NORMAL', 'years_HLY-WCHL-NORMAL', 'HLY-WIND-AVGSPD', 'meas_flag_HLY-WIND-AVGSPD', 'comp_flag_HLY-WIND-AVGSPD', 'years_HLY-WIND-AVGSPD', 'HLY-WIND-PCTCLM', 'meas_flag_HLY-WIND-PCTCLM', 'comp_flag_HLY-WIND-PCTCLM', 'years_HLY-WIND-PCTCLM', 'HLY-WIND-VCTDIR', 'meas_flag_HLY-WIND-VCTDIR', 'comp_flag_HLY-WIND-VCTDIR', 'years_HLY-WIND-VCTDIR', 'HLY-WIND-VCTSPD', 'meas_flag_HLY-WIND-VCTSPD', 'comp_flag_HLY-WIND-VCTSPD', 'years_HLY-WIND-VCTSPD', 'HLY-WIND-1STDIR', 'meas_flag_HLY-WIND-1STDIR', 'comp_flag_HLY-WIND-1STDIR', 'years_HLY-WIND-1STDIR', 'HLY-WIND-1STPCT', 'meas_flag_HLY-WIND-1STPCT', 'comp_flag_HLY-WIND-1STPCT', 'years_HLY-WIND-1STPCT', 'HLY-WIND-2NDDIR', 'meas_flag_HLY-WIND-2NDDIR', 'comp_flag_HLY-WIND-2NDDIR', 'years_HLY-WIND-2NDDIR', 'HLY-WIND-2NDPCT', 'meas_flag_HLY-WIND-2NDPCT', 'comp_flag_HLY-WIND-2NDPCT', 'years_HLY-WIND-2NDPCT']
MEAS_COLUMNS = ['HLY-TEMP-NORMAL', 'HLY-TEMP-10PCTL', 'HLY-TEMP-90PCTL', 'HLY-DEWP-NORMAL', 'HLY-DEWP-10PCTL', 'HLY-DEWP-90PCTL', 'HLY-PRES-NORMAL', 'HLY-PRES-10PCTL', 'HLY-PRES-90PCTL', 'HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'HLY-CLOD-PCTCLR', 'HLY-CLOD-PCTFEW', 'HLY-CLOD-PCTSCT', 'HLY-CLOD-PCTBKN', 'HLY-CLOD-PCTOVC', 'HLY-HIDX-NORMAL', 'HLY-WCHL-NORMAL', 'HLY-WIND-AVGSPD', 'HLY-WIND-PCTCLM', 'HLY-WIND-VCTDIR', 'HLY-WIND-VCTSPD', 'HLY-WIND-1STDIR', 'HLY-WIND-1STPCT', 'HLY-WIND-2NDDIR', 'HLY-WIND-2NDPCT']

def colnames_flags_test():
    for colname in EXPECTED_COLUMNS:
        if colname not in EXPECTED_COLUMNS[:9]:
            if "flag" not in colname and "years" not in colname:
                assert "comp_flag_" + colname in EXPECTED_COLUMNS
                assert "meas_flag_" + colname in EXPECTED_COLUMNS
                assert "years_" + colname in EXPECTED_COLUMNS
    print("PASSED colnames flags test")

def meas_test():
    for meas in MEAS_COLUMNS:
        assert meas in EXPECTED_COLUMNS
        assert "years_" + meas in EXPECTED_COLUMNS
        assert "comp_flag_" + meas in EXPECTED_COLUMNS
        assert "meas_flag_" + meas in EXPECTED_COLUMNS
    print("PASSED meas test")

def colnames_equal_test():
    t0 = time.time()
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        assert np.all(df.columns == EXPECTED_COLUMNS)
        del df # We need to do this so we don't use 1GB of memory
        gc.collect() # We need to do this so we don't use 1GB of memory
        t1 = time.time()
        print(f"colnames_test passed for i={i} in {t1-t0:.2f} seconds")
    print("PASSED colnames equal test")

def comp_flag_values_test():
    t0 = time.time()
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        num_comp_flag_columns = 0
        for colname in df:
            if "comp_flag_" in colname:
                num_comp_flag_columns += 1
                column = df[colname]
                assert np.all(column.isin(["S"," "]))
        assert num_comp_flag_columns == 26 # Yeah this is how many there are
        del df # We need to do this so we don't use 1GB of memory
        gc.collect() # We need to do this so we don't use 1GB of memory
        t1 = time.time()
        print(f"comp_flag_values_test passed for i={i} in {t1-t0:.2f} seconds")
    print("PASSED comp_flag_values_test for every dataframe")

def missing_flag_test():
    t0 = time.time()
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            missing_val  = df[colname] == -9999
            missing_comp = df["comp_flag_" + colname] == ' '
            missing_meas = df["meas_flag_" + colname] == 'M'
            missing_years= df["years_" + colname] == 0
            assert np.all(missing_val == missing_comp)
            assert np.all(missing_val == missing_meas)
            assert np.all(missing_val == missing_years)
        del df # We need to do this so we don't use 1GB of memory
        gc.collect() # We need to do this so we don't use 1GB of memory
        t1 = time.time()
        print(f"missing_flag_test passed for i={i} in {t1-t0:.2f} seconds")
    print("PASSED missing_flag_test for every dataframe")

def get_all_unique_years():
    t0 = time.time()
    unique_years = pd.Series(dtype=int)
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            years_col = df["years_" + colname]
            # print(type(unique_years))
            # print(type(years_col))
            unique_years = pd.concat([unique_years,years_col], ignore_index=True)
            # print(type(unique_years))
            unique_years = pd.Series(unique_years.unique())
            # print(type(unique_years))
        del df # We need to do this so we don't use 1GB of memory
        gc.collect() # We need to do this so we don't use 1GB of memory
        t1 = time.time()
        print(f"unique_years finished for i={i} in {t1-t0:.2f} seconds")
    print("FINISHED unique_years for every dataframe")
    print("Here are the unique years: ")
    for year in unique_years:
        print(year)

def years_col_single_value_test():
    passed = True
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            years_col = df["years_" + colname]
            if len(years_col.unique()) != 1:
                print(f"FAILED years_col_single_value_test for i={i} and colname = {colname}")
                passed = False
        del df # We need to do this so we don't use 1GB of memory
        gc.collect() # We need to do this so we don't use 1GB of memory
    print("FINISHED unique_years for every dataframe")
    if passed:
        print("PASSED years_col_single_value_test for every dataframe")

def x_flag_test():
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            x_flags = df["meas_flag_" + colname] == "X"
            assert np.all(df[x_flags][colname] == 0)
    print("PASSED x flag test")

def vanilla_missing_data_test():
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        assert np.all(df.isna().sum() == 0)
    print("PASSED vanilla missing data test")

def find_culprit_columns():
    culprit_columns = set()
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            if np.any(df[colname] == -9999):
                culprit_columns.add(colname)
    
    print("Columns with anything missing anywhere are: ")
    print(culprit_columns)

    for colname in MEAS_COLUMNS:
        if colname not in culprit_columns:
            print(f"{colname} is not missing anything")

def meas_flag_test():
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        for colname in MEAS_COLUMNS:
            column = df["meas_flag_" + colname]
            assert np.all(column.isin(["M","X"," "]))

    print("PASSED meas_flag_test")

def equal_rows_test():
    for i in range(NUM_LINKS):
        df = get_ith_dataframe(i)
        assert df.shape[0] == 365 * 24
    print("PASSED equal_rows_test")





if False:
    colnames_flags_test()

if False: # This passed
    colnames_equal_test() # Makes sure every dataframe has the same colnames

if False: # This passed
    comp_flag_values_test() # Makes sure every completeness flag is either "S" or " " of every dataframe

if False: # This passed
    meas_test() # Makes sure every measurement colname has a corresponding years_ meas_flag_ and comp_flag_ associated

if False: # This passed
    missing_flag_test() # Makes sure all measurement value == -9999 iff comp_flag_ == " " iff meas_flag_ == "M" iff years_ == 0

if False: # This gives 0,24,25,26,27,28,29,30
    get_all_unique_years() # Gets all values of "years" in all the dataframes

if False: # This fails miserably
    years_col_single_value_test()

if False: # This passed
    x_flag_test() # makes sure that the measurement is 0 whenever the flag is X

if False: # This passed
    vanilla_missing_data_test() # Makes sure that there are no actual missing values (so recorded as "")

if False: # Every column has something missing somewhere
    find_culprit_columns() # This finds all the columns with some data missing in them

if False: # This passed
    meas_flag_test() # Makes sure that the meas_flag_ columns only have values of "M" or "X" or " "

if True: # This passed
    equal_rows_test() # Makes sure that every dataframe has 365 * 24 rows