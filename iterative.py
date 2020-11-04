from statistics import median, mean, stdev

class IterativeDF():
    fi = ""
    delimiter = ""
    columns = []
    fwf_colmap={}
    filt = None
    
    def __init__(self, fi, delimiter=",", columns=[], fwf_colmap={}):
        """
        delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
        columns: for delimited file types, a list of column names in the order they appear
        fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
        start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}
        
        """
        self.fi = fi
        self.delimiter = delimiter
        self.columns = columns
        self.fwf_colmap = fwf_colmap
        
        
    def col_selector(self, row, col):
        """ Selects a column from a row based on file type """

        
        if self.delimiter == "fwf":
            colrange = self.fwf_colmap[col]
            start = colrange[0]
            end = colrange[1]
            return row[start:end]
        else:
            cols = row.split(self.delimiter)
            colindex = self.columns.index(col)
            
            return cols[colindex]
        
    def set_filter(self, col, func):
        """ Sets a row-wise filter to be applied in other methods """
        def filt(row):
            # if no column is set, unset the filter
            if not col:
                self.filt = None
            else:
                # get the value from the row and use it as a filter
                val1 = self.col_selector(row, col)
                if func(val1):
                    return True
                else:
                    return False
        self.filt = filt
        
    def value_counts(self, col):
        """ Gets count of distinct values for a column """
        self.f = open(self.fi, "r")
        vals = {}
        for row in self.f.readlines():
            if not self.filt or self.filt(row):
                
                val = self.col_selector(row, col)
                if val not in vals: vals[val] = 0
                vals[val] = vals[val] + 1
                
        self.f.close()
        return vals
    
    def counts_pct(self, col):
        """ Value counts as a % of total number of rows """
        counts = self.value_counts(col)
        return [(k, val/sum(counts.values())) for k, val in counts.items()]

    def median(self, col):
        """ Creates a median by creating array of all values and then using statistics.median """
        self.f = open(self.fi, "r")
        arr = []
        for row in self.f.readlines():
            if not self.filt or self.filt(row):
                
                val = self.col_selector(row, col)
                try:
                    arr.append(float(val))
                except:
                    pass
        self.f.close()
        return median(arr)
    
    def get_cols(self, cols):
        """ Selects columns from the DF and returns as a dictionary of arrays """
        self.f = open(self.fi, "r")
        arrs = {}
        for col in cols:
            arrs[col] = []
        for row in self.f.readlines():
            if not self.filt or self.filt(row):
                for col in cols:
                    val = self.col_selector(row, col)
                    arrs[col].append(val)
                    
        self.f.close()
        return arrs
    
    def mean(self, col):
        """ Simple average by adding each value and divide by total # of rows """
        
        self.f = open(self.fi, "r")
        total = 0
        rows = 0
        for row in self.f.readlines():
            if not self.filt or self.filt(row):
                
                val = self.col_selector(row, col)
                try:
                    total = total + val
                    rows = rows + 1
                except:
                    pass
        self.f.close()
        
        return total/rows
    
    
    def length(self):
        """ Gets length of DF """
        self.f = open(self.fi, "r")
        length = sum(1 for line in self.f if not self.filt or self.filt(line))
        self.f.close()
        return length
    
    def describe(self, col):
        self.f = open(self.fi, "r")
        arr = []
        total = 0
        rows = 0
        for row in self.f.readlines():
            if not self.filt or self.filt(row):
                
                val = self.col_selector(row, col)
                try:
                    arr.append(val)
                    total = total + val
                    rows = rows + 1
                except:
                    pass
        self.f.close()
        return {
            "Median" :median(arr),
            "Mean": total/rows ,
            "Std": stdev(arr),
            "Min": min(arr),
            "Max": max(arr),
        }
    
