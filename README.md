# iterativedf
Out-of-Core data analysis tool to handle large data sets on limited memory. Loads data in row by row but doesn't hold complete dataset in memory all at once.

Faster than pandas at some tasks, slower at others. Certain methods, like median and other percentiles, are only approximations.

Usage:
import iterativedf as idf

df = idf.read_csv("filepath.csv")

df.head()

df.describe()
