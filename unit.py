### Unit tests

import iterativedf as idf
import time

import pandas as pd


start = time.time()
df = idf.read_csv("sample2.txt", delimiter=",")

print(df.head())

print(df.Price)

df.Price.astype(float)

#print(df.Price.mean())

print(df.Price.describe())


df2 = pd.read_csv("sample2.txt")
print(df2.Price.describe())

#breakpoint()

#df.set_filter("Symbol", lambda x: x == "ONTX")

#breakpoint()
#print(df.head())



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