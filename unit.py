### Unit tests

import iterativedf as idf
import time
import numpy as np

import pandas as pd



def _get_col_dtype(col):
        """
        Infer datatype of a pandas column, process only if the column dtype is object. 
        input:   col: a pandas Series representing a df column. 
        """

        if col.dtype == "object":
            # try numeric
            try:
                col_new = pd.to_datetime(col.dropna().unique())
                return col_new.dtype
            except:
                try:
                    col_new = pd.to_numeric(col.dropna().unique())
                    return col_new.dtype
                except:
                    try:
                        col_new = pd.to_timedelta(col.dropna().unique())
                        return col_new.dtype
                    except:
                        return "object"
        else:
            return col.dtype


start = time.time()
df1 = idf.read_csv("sample2.txt", delimiter=",")

df2 = pd.read_csv("sample2.txt")

tmp1 = df1.head().fillna("")
tmp2 = df2.head(10).fillna("")

#for col in tmp1.columns:
#    if col != "":
#        typ = _get_col_dtype(tmp1[col])
#        tmp1[col] = tmp1[col].astype(typ)
#        tmp2[col] = tmp2[col].astype(typ)



for col in tmp1.columns:
    if col != "":
        #print(col, tmp1[col].dtype, tmp2[col].dtype)
        assert sum(tmp1[col].astype(str) == tmp2[col].astype(str).values) == 10, "Dataframe heads are not the same"


# test for column values with dtype

df1.cols["Price"].func = lambda x: float(x)

df2["Price"] = df2["Price"].astype(float)

tmp1 = df1.head("Price")["Price"]
tmp2 = df2.Price.head(10)


assert np.all(tmp1 == tmp2), "Dataframe column heads are not the same"

assert round(df1.mean("Price"), 10) == round(df2.Price.mean(), 10), "Dataframe means don't match"


assert round(df1.std("Price"), 1) == round(df2.Price.std(), 1), "Dataframe standard deviations don't match"

assert df1.min("Price") == df2.Price.min(), "Dataframe minimums don't match"


assert df1.max("Price") == df2.Price.max(), "Dataframe maximums don't match"

assert df1.length() == len(df2) , "Dataframe lengths don't match"

print("Medians:", df1.median("Price"), df2.Price.median())

#print(df1.describe("Price"))

#print(df2.Price.describe())

# filtered dataframe test

df1.filt = lambda x: x["Symbol"] == "ONTX"

tmp1 = df1.head("Price")["Price"]
tmp2 = df2[df2["Symbol"] == "ONTX"].Price.head(10)

assert np.all(tmp1.values == tmp2.values), "Filtered dataframe column heads are not the same"



#print(df.groupby("Symbol", "Size", "sum").sort_values("sum"))

#df.Avg_Tot_Sbmtd_Chrgs.astype(float)

#df.Avg_Tot_Sbmtd_Chrgs.set_func(lambda x: None if x == '' else x)
#print(df.columns)
#print(df.Avg_Tot_Sbmtd_Chrgs.describe())
#end = time.time()
#print(end - start)

#start = time.time()
#df2 = pd.read_csv("bulk.csv")
#print(df2.Avg_Tot_Sbmtd_Chrgs.describe())
#end = time.time()
#print(end - start)