# iterativedf
Out-of-Core pandas-like data analysis tool to handle large data sets on limited memory. Loads data in row by row but doesn't hold complete dataset in memory all at once.

Faster than pandas at some tasks, slower at others. Certain methods, like median and other percentiles, are only approximations.

Usage:
import iterativedf as idf


## Read in File as CSV
df = idf.read_csv("sample2.txt", delimiter=",")

## First 10 rows
df.head() 

## List of columns in data
df.columns

## Group and aggregate data based on column
df.groupby("Symbol", "Size", "sum").sort_values("sum")

## Limit data output to subset
df.set_filter("Symbol", lambda x: x == "ONTX")

## Get basic stats of a column
df.describe("Price")

## First ten rows of a column
df.head("Date")

## Create a new column based on a function
df.col("Date Of Incident", lambda x: x["dt"][0:4]) 
