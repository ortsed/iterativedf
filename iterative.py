import csv
from statistics import median, mean, stdev
import pandas as pd
import os

class IterativeDF():
	fi = ""
	delimiter = ""
	columns = []
	fwf_colmap = {}
	filt = None
   
	
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
		
		
		if delimiter == "fwf":
			self.columns = fwf_colmap.keys()
		elif len(columns) > 0:
			self.columns = columns
		else: 
			line = next(self.reader())
			self.columns = list(line.keys())

		self.fwf_colmap = fwf_colmap

		class IterativeSeries():
			""" Define the series class for dataframe columns """

			def __init__(self2, col, dtype=None):
				self2.dtype = dtype
				self2.col = col
				self2._clean = None

			def astype(self2, dtype):
				self2.dtype = dtype
				self2.set_clean(eval(self2.dtype))
				
			def set_clean(self2, func):
				self2._clean = func
			
			def clean(self2, x):
				if x:
					return self2._clean(x)
				else:
					return x
								
			def head(self2, ct=10):
				""" Returns first ct number of rows of the series """
				data = []
				i = 0
				for row in self.reader():
					data.append(row)
					i = i + 1
					if i > ct:
						break
				df = pd.DataFrame(data, columns=[self2.col])
				return df 
				
			def values(self2):
				""" Returns values of series as array.  Not optimized for large data """
				
				def _values(row, val):
					if not val:
						val = []
					val.append(self.column(row, self2.col))
					return val
					
				return self.apply(_values)
				
				
			def std(self2):
				""" Standard deviation of a series.  Not optimized for large data """
				return stdev(self2.values)
				
			def median(self2):
				""" 
				Creates an estimated median by creating array of all values 
				and then using statistics.median 
				
				"""
				arr = []
				med = None
				meds = []
				
				for row in self.reader():
					if not self.filt or self.filt(row):
						val = self.column(row, self2.col)
						if val:
							try:
								arr.append(val)
							except:
								pass
							if len(arr) > 1000:
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
						
						val = self.column(row, self2.col)
						try:
							total = total + val
							rows = rows + 1
						except:
							pass
				
				return total/rows
				
			   
			def describe(self2):   
				""" Basic stats of Series """
				
				def _describe(row, vals, col):
				
					if not vals:
						arr = []
						tot = 0
						rows = 0
						meds = []
						mn = None
						mx = None
					else:
						arr, tot, rows, meds, mn, mx = vals
				
					val = self.column(row, col)
					
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
						
						if len(arr) > 1000:
							med = median(arr)
							arr = []
							meds.append(med)
							
					return [arr, tot, rows, meds, mn, mx]
				
				arr, tot, rows, meds, mn, mx  = self.apply(_describe, self2.col)

				return {
					"Median":   median(meds),
					"Mean"  :   tot/rows,
					#"Std":	  	stdev(arr),
					"Min":	  	mn,
					"Max":	 	mx,
					"Rows": 	rows,
					#"total": tot
				
				}
					
			
					
			def value_counts(self2, not_pandas=False, normalize=False):
				""" Gets count of distinct values for a column """
				return self.groupby(self2.col, self2.col, not_pandas=not_pandas, normalize=normalize)
			
			def value_pcts(self2, not_pandas=False):
				""" Value counts as a % of total number of rows """
				return self.groupby(self2.col, self2.col, not_pandas=not_pandas, normalize=True)

		for col in self.columns:
			if col:
				setattr(self, col, IterativeSeries(col))
		
	def groupby(self, col1, col2, method, not_pandas=False, normalize=False):
		""" 
		Group by a column 
		col1: the column being grouped over
		col2: the column whose values are being aggregated
		not_pandas: flag to toggle pandas object output
		normalize: flag to toggle counts as a percentage of total counts
		
		"""

		def _groupby(row, cts_vals, col1, col2, method, not_pandas, normalize):	
			
			if cts_vals == None:
				cts_vals = [{}, {}]
				
			cts, vals = cts_vals			

			val1 = self.column(row, col1)

			if val1 not in cts: 
				cts[val1] = 0
				
			cts[val1] = cts[val1] + 1
			
			if method != "count": 
				
				val2 = self.column(row, col2)
				if val2:									
					if val1 not in vals: 
						vals[val1] = 0
				
					vals[val1] = vals[val1] + val2 
			
			return [cts, vals]
		
		
		cts, vals = self.apply(_groupby, col1, col2, method, not_pandas, normalize)
				
		if method == "count":
			if normalize:
				cts = [(k, val/sum(cts.values())) for k, val in cts.items()]
		
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
			return pd.DataFrame(output.items(), columns=[col1, method])
		
	def apply(self, func, *args, nrows=None):
		""" Method to apply a function across all data through a loop """
		
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
				
				elif self.nrows and i >= nrows + self.skiprows:
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
		
		
	def head(self, ct=10):
		
		def _head(row, vars):
			if vars == None:
				vars = [[], 0]
				
			vals, i = vars
			vals.append(row)
			return [vals, ct]
		
		data, i = self.apply(_head, nrows=ct)

		df = pd.DataFrame(data, columns=self.columns)
		return df 

	def reader(self):
		""" Method for opening and reading the file line by line using DictReader """
		
		f = open(self.file, "r", encoding=self.encoding)
		reader = csv.DictReader(f, delimiter=self.delimiter)
		return reader
		
		
	def column(self, row, col):
		""" Selects a column from a row based on file type """

		if self.delimiter == "fwf":
			colrange = self.fwf_colmap[col]
			start = colrange[0]
			end = colrange[1]
			return row[start:end]
		else:
			val = row[col]
			
			if getattr(self, col)._clean != None:
				
				val = getattr(self, col).clean(val)
				
				
			return val #row[col]
		
	def set_filter(self, col, func):
		""" 
		Sets a row-wise filter to be applied in other methods 
		col: the column the filter is being applied to
		func: a function that accepts a single value and returns a boolean
		for example - func(val) should return True or False
		
		"""
		def filt(row):
			# if no column is set, unset the filter
			if not col:
				self.filt = None
			else:
				# get the value from the row and use it as a filter
				val1 = self.column(row, col)
				if func(val1):
					return True
				else:
					return False
		self.filt = filt


	def get_cols(self, cols, not_pandas=False):
		""" 
		Selects columns from the DF and returns as a dictionary of arrays or pandas DataFrame
		cols: column name or array of column names to be selected
		"""
		arrs = {}
		
		# if only a single col provided as a string, make it an array
		if type(cols) == str:
			cols = [cols]
			
		# Create an empty array for each column
		for col in cols:
			arrs[col] = []
			
		def _get_cols(row, arr, cols):
			return arr.append([self.column(row, col) for col in cols])
			
		arrs = self.apply(_get_cols, cols)			
					
		if not_pandas:
			return arrs
		else:
			return pd.DataFrame(arrs, columns=cols)
	
	def shape(self):
		""" Gets shape of DF with filter"""
		length = self.length()
		return (length, len(self.columns))
		
	def length(self):
		""" Gets shape of DF with filter"""
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




### Unit tests

import time

if __name__ == "__main__":
	start = time.time()
	df = read_csv("sample2.txt", delimiter=",")
	
	df.Size.astype("int")
	
	print(df.Size.describe())
	
	#breakpoint()
	
	#df.set_filter("Symbol", lambda x: x == "ONTX")
	
	#breakpoint()
	#print(df.head())
	
	
	
	print(df.groupby("Symbol", "Size", "sum").sort_values("sum"))
	
	#df.Avg_Tot_Sbmtd_Chrgs.astype(float)
	
	#df.Avg_Tot_Sbmtd_Chrgs.set_clean(lambda x: None if x == '' else x)
	#print(df.columns)
	#print(df.Avg_Tot_Sbmtd_Chrgs.describe())
	#end = time.time()
	#print(end - start)
	
	#start = time.time()
	#df2 = pd.read_csv("bulk.csv")
	#print(df2.Avg_Tot_Sbmtd_Chrgs.describe())
	#end = time.time()
	#print(end - start)
