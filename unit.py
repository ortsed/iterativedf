### Unit tests

import iterativedf as idf
import time
import numpy as np

import pandas as pd

# Read in data using IDF and Pandas

df1 = idf.read_csv("sample2.txt", delimiter=",")

df2 = pd.read_csv("sample2.txt")


# Test column values are the same

tmp1 = df1.head().fillna("")
tmp2 = df2.head().fillna("")


for col in tmp1.columns:
    if col != "":
        #print(col, tmp1[col].dtype, tmp2[col].dtype)
        assert sum(tmp1[col].astype(str) == tmp2[col].astype(str).values) == 5, "Dataframe heads are not the same"


# Test for column values with dtype

df1.cols["Price"].func = lambda x: float(x)

df2["Price"] = df2["Price"].astype(float)

tmp1 = df1.head("Price")["Price"]
tmp2 = df2.Price.head()

assert np.all(tmp1 == tmp2), "Dataframe column heads are not the same"

# Test Series mean

assert round(df1.mean("Price"), 10) == round(df2.Price.mean(), 10), "Dataframe means don't match"

# Test series standard deviation

assert round(df1.std("Price"), 1) == round(df2.Price.std(), 1), "Dataframe standard deviations don't match"

# Test Series minimum

assert df1.min("Price") == df2.Price.min(), "Dataframe minimums don't match"

# Test series maximum

assert df1.max("Price") == df2.Price.max(), "Dataframe maximums don't match"

# test dataframe length

assert df1.length() == len(df2) , "Dataframe lengths don't match"


# Show medians - 

med1 = df1.median("Price")
med2 = df2.Price.median()
med_error = (med1 - med2)/med1

assert abs(med_error) < .01, "Approximate median is not within 1% tolerance"

# filtered dataframe test

df1.set_filter(lambda x: x["Symbol"] == "ONTX")

tmp1 = df1.head("Price")["Price"]
tmp2 = df2[df2["Symbol"] == "ONTX"].Price.head()

assert np.all(tmp1.values == tmp2.values), "Filtered dataframe column heads are not the same"


# Test Group By

df1.set_filter(None)
df1.set_func("Size", lambda x: float(x))

tmp1 = df1.groupby("Symbol", "Size", "sum").sort_values(["sum", "Symbol"])


tmp2 = df2.groupby("Symbol")["Size"].sum().reset_index().sort_values(["Size", "Symbol"])

assert np.all(tmp1.values == tmp2.values), "Grouped dataframe values do not match"

# Test column names
# Differs from Pandas on how it handles blank columns: to fix

#assert list(df1.columns) == list(df2.columns), "Column values do not match"


# Clean and Describe functions

def clean_price(x):
    if x == "":
        return 0
    else:
        try:
            return float(x)
        except:
            return 0
df1.set_func("Price", clean_price)
#df1.values("ShortType")


print(df1.describe("Price"))
