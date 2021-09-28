import csv
from statistics import median, mean, stdev
import pandas as pd


class IterativeDF():
    fi = ""
    delimiter = ""
    columns = []
    fwf_colmap={}
    filt = None
    
    def __init__(self, file, delimiter=",", columns=[], fwf_colmap={}, encoding=None):
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
        
        if delimiter == "fwf":
            self.columns = fwf_colmap.keys()
        elif len(columns) > 0:
            self.columns = columns
        else: 
            line = next(self.reader())
            self.columns = list(line.keys())

        self.fwf_colmap = fwf_colmap
        
        self.f = open(self.file, "r")
        
    def lines(self):
        return self.f.readlines()[1:]
        
    def head(self, ct=10):
        data = []
        i = 0
        for row in self.reader():
            data.append(row)
            i = i +1
            if i > ct:
                break
        df = pd.DataFrame(data, columns=self.columns)
        return df 

    def reader(self):
        f = open(self.file, "r", encoding=self.encoding)
        reader = csv.DictReader(f)
        return reader
        
    def column(self, row, col):
        """ Selects a column from a row based on file type """

        if self.delimiter == "fwf":
            colrange = self.fwf_colmap[col]
            start = colrange[0]
            end = colrange[1]
            return row[start:end]
        else:
            cols = row #row.split(self.delimiter)
            
            colindex = self.columns.index(col)
            
            return cols[colindex] #row[col]
        
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
        
    def value_counts(self, col, not_pandas=False, normalize=False):
        """ Gets count of distinct values for a column """
        if normalize:
            return self.value_pcts(col, not_pandas)
        
        vals = {}
        for row in self.lines():
            if not self.filt or self.filt(row):
                val = self.column(row, col)
                
                if val not in vals: vals[val] = 0
                
                vals[val] = vals[val] + 1

        if not_pandas:
            return vals
        else:
            return pd.DataFrame(vals.items(), columns=[col, "counts"])
    
    def value_pcts(self, col, not_pandas=False):
        """ Value counts as a % of total number of rows """
        counts = self.value_counts(col, not_pandas=True)
        
        pcts = [(k, val/sum(counts.values())) for k, val in counts.items()]

        if not_pandas:
            return pcts
        else:
            return pd.DataFrame(pcts, columns=[col, "pct"])

    def get_cols(self, cols, not_pandas=False):
        """ 
        Selects columns from the DF and returns as a dictionary of arrays or pandas DataFrame
        cols: column name or array of column names to be selected
        """
        arrs = {}
        
        if type(cols) == str:
            cols = [cols]
            
        for col in cols:
            arrs[col] = []
            
        for row in self.lines():
            if not self.filt or self.filt(row):
                for col in cols:
                    val = self.column(row, col)
                    arrs[col].append(val)
                    
        if not_pandas:
            return arrs
        else:
            return pd.DataFrame(arrs, columns=cols)

    def median(self, col):
        """ Creates a median by creating array of all values and then using statistics.median """
        arr = []
        for row in self.lines():
            if not self.filt or self.filt(row):
                val = self.column(row, col)
                try:
                    arr.append(float(val))
                except:
                    pass
        return median(arr)
         
    
    def mean(self, col):
        """ Simple average by adding each value and divide by total # of rows """

        total = 0
        rows = 0
        for row in self.lines():
            if not self.filt or self.filt(row):
                
                val = self.column(row, col)
                try:
                    total = total + val
                    rows = rows + 1
                except:
                    pass
        
        return total/rows
    
    
    def length(self):
        """ Gets length of DF """
        length = sum(1 for line in self.f if not self.filt or self.filt(line))
        return length
    
    def describe(self, col):
 
        arr = []
        total = 0
        rows = 0
        for row in self.lines():
            if not self.filt or self.filt(row):
                
                val = self.column(row, col)
                try:
                    arr.append(val)
                    total = total + val
                    rows = rows + 1
                except:
                    pass

        return {
            "Median":   median(arr),
            "Mean"  :   total/rows ,
            "Std":      stdev(arr),
            "Min":      min(arr),
            "Max":      max(arr),
        }
    
def read_csv(file, delimiter=",", columns=[], fwf_colmap={}, encoding=None):
    """

    delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
    columns: for delimited file types, a list of column names in the order they appear
    fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
    start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}

    """
    df = IterativeDF(file, delimiter=",", columns=[], fwf_colmap={}, encoding=encoding)
    return df


if __name__ == "__main__":
    df = read_csv("MUP_OHP_R19_P04_V40_D16_Prov_Svc.csv")
    #print(df.head())
    print(df.value_counts("Rndrng_Prvdr_Org_Name"))
