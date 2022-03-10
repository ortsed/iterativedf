# iterativedf
Pandas-like data tool for analyzing data iteratively to avoid memory issues.

For large data being run on machines that have limited memory/CPU like Vaex. Loads data in row by row but doesn't hold complete dataset in memory all at once.

Faster than pandas at some tasks, slower at others. Certain methods, like median and other percentiles, are only approximations.

Usage:
import iterativedf as idf

df = idf.read_csv("filepath.csv")

df.head()

df.describe()
