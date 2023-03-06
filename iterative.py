import csv
from statistics import median, mean, stdev
import pandas as pd
import os

set_option = pd.set_option


"""
Class that defines a dataframe similar to Pandas
Data is not read into memory, but loops through row by row


# Read in File as CSV
df = idf.read_csv("sample2.txt", delimiter=",")

# First 10 rows
df.head() 

# List of columns in data
df.columns

# Group and aggregate data based on column
df.groupby("Symbol", "Size", "sum").sort_values("sum")

# Limit data output to subset
df.set_filter("Symbol", lambda x: x == "ONTX")
	

# Series methods
df.Size.astype("int")

# Get basic stats of a column
df.Size.describe()

# First ten rows of a column
df.__dict__['Date Of Incident'].head()

# Create a new column based on a calculation of another column
df.__dict__['Date Of Incident'].cp("dt", lambda x: x[0:4]) 

df.



"""

class IterativeDF():
	fi = ""
	delimiter = ""
	columns = []
	fwf_colmap = {}
	filt = None
	func = None
   
	
	def __init__(self, file, delimiter=",", columns=[], fwf_colmap={}, encoding=None, dtypes={}, nrows=None, skiprows=0):
		"""
		delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
		
		columns: for delimited file types, a list of column names in the order they appear
		
		fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
		start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}
		
		encoding: utf-8 vs others
		
		"""
		self.file = file
		self.delimiter = delimiter
		self.encoding = encoding
		self.dtypes = dtypes
		self.nrows = nrows
		self.skiprows = skiprows
		self.fwf_colmap = fwf_colmap
		
		def __getitem__(self, key):
			return getattr(self, key)
		
		
		if delimiter == "fwf":
			self.columns = fwf_colmap.keys()
		elif len(columns) > 0:
			self.columns = columns
		else: 
			line = next(self.reader())
			self.columns = list(line.keys())
			

		class IterativeSeries():
			""" Define the series class for dataframe columns """

			def __init__(self2, column, handle=None, dtype=None):
				self2.dtype = dtype
				
				# Name of the column in the original data
				self2.column = column 
				
				# What the column is referred to in the idf
				# Changes when there is a calculated field
				if handle:
					self2.handle = handle
				else:
					self2.handle = column
				
				self2.func = None
				
			def __str__(self2):
				return str(self2.head())
				

			def astype(self2, dtype):
				self2.dtype = dtype
				self2.func = lambda x: dtype(x)
				
					
			def cp(self2, col_name, func=None):
				self.__dict__[col_name] = IterativeSeries(self2.column, handle=col_name)
				
				if func:
					self.__dict__[col_name].func = func
								
			def head(self2, nrows=10):
				""" Return top n number of rows """
				return self2.values(nrows=nrows)
				
			def values(self2, nrows=10):
				""" Returns values of series as array.  """
				
				def _values(row, val):
					if not val:
						val = []
					data = self.column(row, self2.column)
					
					
					if self2.func:
						data = self2.func(data)
					
					val.append(data)
					
					return val
				
				data = self.apply(_values, nrows=nrows)
				
				return pd.DataFrame(data, columns=[self2.handle])
				
				
			def std(self2):
				""" Standard deviation of a series.  
				todo: Needs to be turned into an iterative method
				Not optimized for large data 
				"""
				return stdev(self2.values)
				
			def median(self2, subgroup_size=1000):
				""" 
				Creates an estimated median by calculating median of subarrays
				and then calculating median of array of medians 
				
				"""
				arr = []
				med = None
				meds = []
				
				for row in self.reader():
					if not self.filt or self.filt(row):
						val = self.column(row, self2.column)
						if val:
							try:
								arr.append(val)
							except:
								pass
							if len(arr) > subgroup_size:
								med = median(arr)
								arr = []
								meds.append(med)
							   
				return median(meds)
				 
			
			def mean(self2):
				""" Simple average by adding each value and divide by total # of rows """

				total = 0
				rows = 0
				for row in self.reader():
					if not self.filt or self.filt(row):
						
						val = self.column(row, self2.column)
						try:
							total = total + val
							rows = rows + 1
						except:
							pass
				
				return total/rows
				
			   
			def describe(self2):   
				""" Basic stats of Series """
				
				def _describe(row, vals, column):
				
					if not vals:
						arr = []
						tot = 0
						rows = 0
						meds = []
						mn = None
						mx = None
					else:
						arr, tot, rows, meds, mn, mx = vals
				
					val = self.column(row, column)
					
					if val:
						tot += val
						
						rows += 1
						arr.append(val)
						
						if not mn:
							mn = mx = val
						
						if val > mx:
							mx = val
						elif val < mn:
							mn = val
						
						if len(arr) > 10:
							med = median(arr)
							arr = []
							meds.append(med)

					if len(arr) > 0:
						med = median(arr)
						meds.append(med)
							
					return [arr, tot, rows, meds, mn, mx]
				
				arr, tot, rows, meds, mn, mx  = self.apply(_describe, self2.column)

				return pd.Series({
					"count": 	rows,
					"mean":   	tot/rows,
					"min":	  	mn,
					"50%":   	median(meds),
					"max":	 	mx,
					#"Std":	  	stdev(arr),
					#"total": tot
				})
					
			
					
			def value_counts(self2, not_pandas=False, normalize=False):
				""" Gets count of distinct values for a column """
				return self.groupby(self2.column, self2.column, "count", not_pandas=not_pandas, normalize=normalize)
			
			def value_pcts(self2, not_pandas=False):
				""" Value counts as a % of total number of rows """
				return self.groupby(self2.column, self2.column, "count", not_pandas=not_pandas, normalize=True)

		# sets a Series attribute for each column
		for column in self.columns:
			if column:
				setattr(self, column, IterativeSeries(column))
				self.__dict__[column] = IterativeSeries(column)
				
	def groupby(self, column1, column2, method, not_pandas=False, normalize=False):
		""" 
		Group by a column 
		col1: the column being grouped over
		col2: the column whose values are being aggregated
		not_pandas: flag to toggle pandas object output
		normalize: flag to toggle counts as a percentage of total counts
		
		todo: group by multiple columns
		
		"""

		def _groupby(row, cts_vals, column1, column2, method, not_pandas, normalize):	
			
			if cts_vals == None:
				cts_vals = [{}, {}]
				
			cts, vals = cts_vals			

			val1 = self.column(row, column1)

			if val1 not in cts: 
				cts[val1] = 0
				
			cts[val1] = cts[val1] + 1
			
			if method != "count": 
				
				val2 = self.column(row, column2)
				if val2:									
					if val1 not in vals: 
						vals[val1] = 0
				
					vals[val1] = vals[val1] + val2 
			
			return [cts, vals]
		
		
		cts, vals = self.apply(_groupby, column1, column2, method, not_pandas, normalize)
				
		if method == "count":
			if normalize:
				cts = {k: val/sum(cts.values()) for k, val in cts.items()}
		
			output = cts
		elif method == "mean":
			
			for k,v in vals.items():
				vals[k] = vals[k]/cts[k]
			
			output = vals
			
		elif method == "sum":
			output = vals
		
		if not_pandas:
			return output
		else:
			return pd.DataFrame(output.items(), columns=[column1, method])
		
	def apply(self, func, *args, nrows=None):
		""" Method to apply a function across all data in the dataframe """
		
		vals = None
			
		# define total rows if counting
		if not nrows and self.nrows:
			nrows = self.nrows

		
		i = 0
		for row in self.reader():
			if not self.filt or self.filt(row):
				
				if self.skiprows and i <= self.skiprows:
					# do nothing if skipping first X rows
					pass
				
				elif nrows and i >= nrows + self.skiprows:
				# Break if past the total number of rows
					break
				
				else:
					# vals is a variable that persists through the loop
					# any function that is used by apply must have this same attribute format
					# def func(row, vals, *args):
					# 	return vals
					
					vals = func(row, vals, *args)
					 
				i = i + 1
		return vals
		
		
	def head(self, nrows=10):
		""" Return top n number of rows """
		
		def _head(row, vars):
			if vars == None:
				vars = [[], 0]
				
			vals, i = vars
			vals.append(row)
			return [vals, nrows]
		
		data, i = self.apply(_head, nrows=nrows)

		df = pd.DataFrame(data, columns=self.columns)
		return df 

	def reader(self):
		""" Method for opening and reading the file line by line using csv.DictReader """
		
		f = open(self.file, "r", encoding=self.encoding)
		reader = csv.DictReader(f, delimiter=self.delimiter)
		return reader
		
		
	def column(self, row, column):
		""" Selects a column from a row """

		if self.delimiter == "fwf":
			colrange = self.fwf_colmap[column]
			start = colrange[0]
			end = colrange[1]
			return row[start:end]
		else:
			val = row[column]
			
			if getattr(self, column).func != None:
				
				val = getattr(self, column).func(val)
				
				
			return val #row[col]
		
	def set_filter(self, column, func):
		""" 
		Sets a row-wise filter to be applied in other methods 
		col: the column the filter is being applied to
		func: a function that accepts a single value and returns a boolean
		for example - func(val) should return True or False
		
		"""
		def filt(row):
			# if no column is set, unset the filter
			if not column:
				self.filt = None
			else:
				# get the value from the row and use it as a filter
				val1 = self.column(row, column)
				if func(val1):
					return True
				else:
					return False
		self.filt = filt

	def get_cols(self, cols, not_pandas=False):
		""" 
		Selects columns from the dataframe and returns as a dictionary of arrays or pandas DataFrame
		cols: column name or array of column names to be selected
		"""
		arrs = {}
		
		# if only a single col provided as a string, make it an array
		if type(cols) == str:
			cols = [cols]
			
		# Create an empty array for each column
		for column in cols:
			arrs[column] = []
			
		def _get_cols(row, arr, cols):
			return arr.append([self.column(row, column) for column in cols])
			
		arrs = self.apply(_get_cols, cols)			
					
		if not_pandas:
			return arrs
		else:
			return pd.DataFrame(arrs, columns=cols)
	
	def shape(self):
		""" Gets shape of dataframe with filter"""
		length = self.length()
		return (length, len(self.columns))
		
	def length(self):
		""" Gets shape of dataframe with filter"""
		return sum(1 for line in self.reader() if not self.filt or self.filt(line))


### Common Utility functions


def read_csv(file, delimiter=",", columns=[], fwf_colmap={}, encoding=None, nrows=None, skiprows=0):
	"""
	delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
	columns: for delimited file types, a list of column names in the order they appear
	fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
	start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}
	""" 
	df = IterativeDF(file, delimiter=delimiter, columns=columns, fwf_colmap=fwf_colmap, encoding=encoding, nrows=nrows, skiprows=skiprows)
	return df







