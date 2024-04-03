import csv
from statistics import median, mean, stdev
import pandas as pd
import os, math

set_option = pd.set_option


"""
IterativeDF class that defined a dataframe similar to Pandas.
Data is not read into memory, but loops through row by row.

Sample methods

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

# Get basic stats of a column
df.Size.describe()

# First ten rows of a column
df.head("Date")

# Create a new column based on calculation from that row
df.col("Date Of Incident", lambda x: x["dt"][0:4]) 




"""

class FWFReader():
	"""
	Document reader for fixed-width files
	similar to csv.DictReader
	requires a fwf_colmap in the form of:
	{
		"column1": [0,4],
		"column2": [5:8],
		....
	}

	"""
	def __init__(self, f, fwf_colmap, restkey=None, restval=None,
				 dialect="excel", *args, **kwds):
		self._fieldnames = fwf_colmap.keys()   # list of keys for the dict
		self._ranges = fwf_colmap.values()
		self.restkey = restkey		  # key to catch long rows
		self.restval = restval		  # default value for short rows
		self.reader = csv.reader(f, dialect, *args, **kwds)
		self.dialect = dialect
		self.line_num = 0

	def __iter__(self):
		return self

	@property
	def fieldnames(self):
		"""
			gets the columns of the dataframe as an array
		"""
	
		if self._fieldnames is None:
			try:
				self._fieldnames = next(self.reader)
			except StopIteration:
				pass
		self.line_num = self.reader.line_num
		return self._fieldnames

	@fieldnames.setter
	def fieldnames(self, value):
		""" Sets the value of the dataframe columns """
		self._fieldnames = value
		
	def __next__(self):
		row = next(self.reader)
		row2 = []
		[row2.append(row[0][x:y]) for x,y in self._ranges]
		row = row2
		
		self.line_num = self.reader.line_num

		# unlike the basic reader, we prefer not to return blanks,
		# because we will typically wind up with a dict full of None
		# values
		while row == []:
			row = next(self.reader)
		d = dict(zip(self.fieldnames, row))
		lf = len(self.fieldnames)
		lr = len(row)
		if lf < lr:
			d[self.restkey] = row[lf:]
		elif lf > lr:
			for key in self.fieldnames[lr:]:
				d[key] = self.restval
		return d

class IterativeSeries():
	""" Define the series class for dataframe columns """

	def __init__(self2, column, handle=None):
		
		# Name of the column in the original data
		self2.column = column 
		
		# What the column is referred to in the idf
		# changes when there is a calculated field
		if handle:
			self2.handle = handle
		else:
			self2.handle = column
			
		
		# Default get method
		# gets the value from the row	
		self2.get = lambda x: x[column]
		
		# Default cleaning method
		# used to set the datatype
		self2.func = None
		
	def __str__(self2):
		# output the head of the dataframe as default
		return str(self2.head())
		

class IterativeDF():
	"""
	Base class for dataframes.
	Holds methods for dataframe wide methods.
	All other methods are within the IterativeSeries class.
	"""
	fi = "" 
	delimiter = ""
	columns = []
	fwf_colmap = {}
	filt = None
	func = None
   
	
	def __init__(self, file, delimiter=",", columns=[], fwf_colmap={}, encoding=None, dtypes={}, nrows=None, skiprows=0):
		"""
		delimiter: determines type of separated file, 
		such as ",", "\t", "|" -- or "fwf" for fixed with files
		
		columns: for delimited file types, a list of column 
		names in the order they appear
		
		fwf_colmap: for fixed width files, a dictionary that maps 
		column name to a list of start and endpoints for that 
		column {'colname': [0,2], 'colname2': [3,4]}
		
		encoding: utf-8 vs others
		
		"""
		self.file = file
		self.delimiter = delimiter
		self.encoding = encoding
		self.dtypes = dtypes
		self.nrows = nrows
		self.skiprows = skiprows
		self.fwf_colmap = fwf_colmap
		
		# Filter to be applied on dataframe
		# a function whose input is a row
		# that determines if row is ignored
		# if True row is included
		# e.g. df.filt = lambda x: x["date"] > 2004
		self.filt = None  
		
		self.cols = {}
		
		def __getitem__(self, key):
			return getattr(self, key)
		
		# if fixed width format
		if delimiter == "fwf":
			self.columns = fwf_colmap.keys()
		elif len(columns) > 0:
			# if columns are defined, use them
			self.columns = columns
		else: 
			# if columns attribute is not defined
			# get columns based on first row
			
			line = next(self.reader())
			
			self.columns = list(line.keys())				


		# sets a Series attribute for each column
		for column in self.columns:
			if column:
				self.cols[column] = IterativeSeries(column)
				
		
	def set_filter(self, func):
		"""
		Defines a function to filter rows from the whole dataframe
		applied for all other functions that use the dataset
		Set to None to undo
		
		
		ex: df.set_filter(lambda x: x["name"] != None)
		df.set_filter(None)
		
		"""
		self.filt = func
		return None
		
	def col(column_name, func):
		"""
		Define a calculated column based on a function
		
		ex: df.col("calc_val", lambda x: x["price"] * 3)
		
		"""
	
		if column_name in self.columns:
			raise("Column name already exists")
		else:
			self.cols[column] = IterativeSeries(column)
			
			# rather than basic get() method, calculated get() is based on values 
			# from other columns
			self.cols[column].get = func
			
	
	def groupby(self, column1, column2, method, not_pandas=False, normalize=False):
		""" 
		Group by a column 
		column1: the column being grouped over
		column2: the column whose values are being aggregated
		not_pandas: flag to toggle pandas object output
		normalize: flag to toggle counts as a percentage of total counts
		
		current method choices: sum, mean, max, min, median
		
		todo: group by multiple columns
		
		"""

		def _groupby(row, cts_vals, column1, column2, method, not_pandas, normalize):	
			# submethod for looping
			
			# define dict of counts/values if not already passed
			if cts_vals == None:
				cts_vals = [{}, {}]
			
			cts, vals = cts_vals
			
			# determine if grouping on multiple values
			multi_group = True if type(column1) == list else False
			
			# if multiple groupings
			if multi_group:
				agg_key1 = 	column1[0]
				agg_key2 = 	column1[1]
				agg_key_val1 = self.column(row, agg_key1)
				agg_key_val2 = self.column(row, agg_key2)
				val1 = self.column(row, agg_key1)
				
				if agg_key_val1 not in cts: 
					cts[agg_key_val1] = {}
				
				if agg_key_val2 not in cts[agg_key_val1]: 
					cts[agg_key_val1][agg_key_val2] = 0
					
				cts[agg_key_val1][agg_key_val2] = cts[agg_key_val1][agg_key_val2] + 1
				
			else:
				# get the value for that row
				agg_key1 = column1

				agg_key_val1 = self.column(row, agg_key1)
		
				# if the value hasn't been seen yet
				# set count to 0, otherwise add 1
				
				if agg_key_val1 not in cts: 
					cts[agg_key_val1] = 0
				
				cts[agg_key_val1] = cts[agg_key_val1] + 1
				
			
			# for all methods besides median
			if method in ["sum", "mean", "max", "min"]: 
				
				# get the value of that row
				agg_val = self.column(row, column2)
				
				
				if agg_val:	
					# if grouping over multiple values
					if multi_group:
						if agg_key_val1 not in vals: 
							vals[agg_key_val1] = {}
				
						if agg_key_val2 not in vals[agg_key_val1]: 
							vals[agg_key_val1][agg_key_val2] = 0
							
						vals[agg_key_val1][agg_key_val2] = vals[agg_key_val1][agg_key_val2] + agg_val
						
					else:						
						if agg_key_val1 not in vals: 
							vals[agg_key_val1] = 0
				
						vals[agg_key_val1] = vals[agg_key_val1] + agg_val
						
			elif method == "median":
				# medians have to be handled differently
				# values are appended to an array, and then 
				# averaged at the end
				
				agg_val = self.column(row, column2)
				if agg_val:		
					if agg_key_val1 not in vals: 
						vals[agg_key_val1] = []
				
					vals[agg_key_val1].append(agg_val)
			
			return [cts, vals]
		
		# apply the looped method
		cts, vals = self.apply(_groupby, column1, column2, method, not_pandas, normalize)
		
		# apply the final aggregation function
		
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
			
		elif method == "max":
			output = max(vals.values())
		elif method == "min":
			output = min(vals.values())
		elif method == "median":
			for k,v in vals.items():
				vals[k] = median(vals[k])
			output = vals
		
		if not_pandas:
			return output
		else:
			return pd.DataFrame(output.items(), columns=[column1, method])
		
	def apply(self, func, *args, start=0, nrows=None):
		""" 
		Method to apply a function across all data in the dataframe 
		Similar to pandas apply() method
		"""
		
		vals = None
			
		# define total rows if counting
		if not nrows and self.nrows:
			nrows = self.nrows

		
		i = 0
		# loop through the file reader
		for row in self.reader():
		
			# if a filter is defined
			if not self.filt or self.filt(row):
				
				# do nothing if skipping rows
				if self.skiprows and i <= self.skiprows:
					pass
				
				# Break if past the total number of rows
				elif nrows and i >= nrows + self.skiprows:
				
					break
				
				else:
					# vals is a variable that persists through the loop
					# any function that is used by apply must have this same attribute format
					# def func(row, vals, *args):
					# 	return vals
					
					vals = func(row, vals, *args)
					 
				i = i + 1
		return vals
		
	def unique(self, column):
		""" Calculate unique/distinct values in a column """

		def _unique(row, vals):
			if vals == None:
				vals = []
			
			val = self.column(row, column)
			
			if val not in vals:
				vals.append(val)
				
			return vals
	
		return self.apply(_unique)
		
	def head(self, column=None, nrows=5, sort=False, ascending=False):
		""" Return the top n number of rows """
		
		# if a column is passed, return top n rows of the series
		# using series method
		if column:
			return self.values(column, nrows=nrows, sort=sort, ascending=ascending)		
		
		# if no column, return top n rows of dataframe
		else:
			# define submethod for looping
			def _head(row, vars):
				if vars == None:
					vars = [[], 0]
				
				vals, i = vars
				vals.append(row)
				return [vals, nrows]
		
			tmp = self.apply(_head, nrows=nrows)
			if tmp:
				data, i = tmp
			else:
				data, i = None, None

			df = pd.DataFrame(data, columns=self.columns)
			return df 

	def reader(self):
		""" Method for opening and reading the file line by line using csv.DictReader """
		f = open(self.file, "r", encoding=self.encoding)
		
		if self.fwf_colmap:
			reader = FWFReader(f, fwf_colmap=self.fwf_colmap)
		else:
			reader = csv.DictReader(f, delimiter=self.delimiter)
			
		return reader
		
		
	def column(self, row, column):
		""" 
		Selects column data from a row
		
		"""
		
		# if a fixed width file
		
		if self.delimiter == "fwf":
			
			return row[column]
		
		# not a fixed width file
		else:
			# get the series object by its handle
			series = self.cols[column]
			
			# get the column value using .get() method
			val = series.get(row)
		
			# apply function if exists
			if series.func != None:
				val = series.func(val)
				
				
			return val
		


	def get_cols(self, cols, not_pandas=False):
		""" 
		Selects columns from the dataframe and returns a dictionary of arrays 
		or pandas DataFrame
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
	
	@property
	def shape(self):
		""" Gets shape of dataframe with filter applied """
		length = self.length()
		return (length, len(self.columns))
		
	def length(self):
		""" Gets shape of dataframe with filter applied """
		return sum(1 for line in self.reader() if not self.filt or self.filt(line))
		
		
	def values(self, column, start=0, nrows=10, sort=False, ascending=False):
		""" Returns values of series as array. Essentially a subset of dataframe """
		
		if sort:
			_nrows = nrows
			nrows = None
			
		def _values(row, val):
			if not val:
				val = []
				
			data = self.column(row, column)
			
			val.append(data)
			
			if sort:
				reverse = not ascending
				val = sorted(val, reverse=reverse)[start:nrows]
			
			return val
		
		data = self.apply(_values, start=start, nrows=nrows)
		
		return pd.DataFrame(data, columns=[column])
		
	def min(self, column):
		
		
		def _min(row, mn):
			if not mn:
				mn = None
			
			data = self.column(row, column)
			
			if not mn or data < mn:
				mn = data
				
			return mn
		
		return self.apply(_min)
		
	def max(self, column):
		
		
		def _max(row, mx):
			if not mx:
				mx = None
			
			data = self.column(row, column)
			
			if not mx or data > mx:
				mx = data
				
			return mx
		
		return self.apply(_min)
	
	def std(self, column):
		""" 
		Standard deviation of a series.  
		todo: Needs to be turned into an iterative method
		Not optimized for large data 
		"""
		
		# get the average of the column
		mean = self.mean(column)
		
		# define the looped method
		def _std(row, vals, mean=mean):
			if not vals:
				vals = []
			
			# get the column value
			data = self.column(row, column)
			
			# apply std formula
			val = (data - mean)**2
			
			# add to the array
			vals.append(val)
			
			return vals
		
		data = self.apply(_std)
		
		std_total = math.sqrt(sum(data)/self.length())
		
		return std_total
		
	def median(self, column, subgroup_size=1000):
		""" 
		Creates an estimated median by calculating median of subarrays
		and then calculating median of array of medians 
		
		"""
		
		# define looped method
		def _median(row, vals):
			
			
			if not vals: 
				vals = [[], []]
			
			vals1, medians = vals
			
			val = self.column(row, column)
			
			if val:
				try:
					vals1.append(val)
				except:
					pass
				if len(vals1) > subgroup_size:
					med = median(vals1)
					vals1 = []
					medians.append(med)
					
			return [vals1, medians]
				
		medians = self.apply(_median)	
		
		   
		return median(medians[0])
		 
	
	def mean(self, column):
		""" Simple average by adding each value and divide by total # of rows """
		
		def _mean(row, vals):
			if not vals: 
				vals = [0, 0]
			
			total, counts = vals
			
			val = self.column(row, column)
			try:
				total = total + val
				counts = counts + 1
			except:
				pass
			return [total, counts]
					
		total, counts = self.apply(_mean)
		
		return total/counts if counts != 0 else 0
	
	def max(self, column):

		""" Get the maximum value of a series """
	
		# define looped method
		def _max(row, _max_val):
			if not _max: 
				_max_val = None
		
			val = self.column(row, column)
		
			if not _max_val or val > _max_val:
				_max_val = val  

			return _max_val
		
		# apply looped method
		_max_val = self.apply(_max)
	
		return _max_val
		
	   
	def describe(self, column):   
		""" Basic stats of Series """
		
		# define looped method
		def _describe(row, vals, column):
		
			# define default values
			if not vals:
				arr = []
				tot = 0
				rows = 0
				meds = []
				mn = None
				mx = None
			else:
				arr, tot, rows, meds, mn, mx = vals
			
			# get current value of column
			val = self.column(row, column)
			
			
			# append values to arrays
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
		
		# apply looped method
		arr, tot, rows, meds, mn, mx  = self.apply(_describe, column)

		return pd.Series({
			"count": 	rows,
			"mean":   	tot/rows if rows != 0 else 0,
			"std":	  	self.std(column),
			"min":	  	mn,
			"50%":   	median(meds),
			"max":	 	mx,
		})
			
	
			
	def value_counts(self, column, not_pandas=False, normalize=False):
		""" Gets count of distinct values for a column """
		return self.groupby(column, column, "count", not_pandas=not_pandas, normalize=normalize)
	
	def value_pcts(self, column, not_pandas=False):
		""" Value counts as a % of total number of rows """
		return self.groupby(column, column, "count", not_pandas=not_pandas, normalize=True)
		



def read_csv(file, delimiter=",", columns=[], fwf_colmap={}, encoding=None, nrows=None, skiprows=0):
	"""
	Reads in a file and returns an IDF dataframe
	
	delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
	columns: for delimited file types, a list of column names in the order they appear
	fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
	start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}
	""" 
	df = IterativeDF(file, delimiter=delimiter, columns=columns, fwf_colmap=fwf_colmap, encoding=encoding, nrows=nrows, skiprows=skiprows)
	
	return df