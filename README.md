# iterativedf
Pandas-like data tool for analyzing data iteratively to avoid memory issues.

For large data being run on machines that have limited memory/CPU. Loads data in row by row.

Usage:

from iterativedf import IterativeDF

df = IterativeDF("filepath.csv")

df.head()

df.describe()
