import csv
from statistics import median, mean, stdev
import pandas as pd


class IterativeDF():
    fi = ""
    delimiter = ""
    columns = []
    fwf_colmap={}
    filt = None
   
    
    def __init__(self, file, delimiter=",", columns=[], fwf_colmap={}, encoding=None, dtypes={}):
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
        
        
        if delimiter == "fwf":
            self.columns = fwf_colmap.keys()
        elif len(columns) > 0:
            self.columns = columns
        else: 
            line = next(self.reader())
            self.columns = list(line.keys())

        self.fwf_colmap = fwf_colmap
                
        class IterativeSeries():
            def __init__(self2, col, dtype=None):
                self2.dtype = dtype
                self2.col = col
                self2._clean = None
            
            def value_counts(self2, **kwargs):
                return self.value_counts(self2.col, kwargs) 

            def astype(self2, dtype):
                self2.dtype = dtype
                
            def set_clean(self2, func):
                self2._clean = func
            
            def clean(self2, x):
                return self2._clean(x)
                
            def _astype(self2, x):
                return self2.dtype(x)
                
            def head(self2, ct=10):
                data = []
                i = 0
                for row in self.reader():
                    data.append(row)
                    i = i +1
                    if i > ct:
                        break
                df = pd.DataFrame(data, columns=[self2.col])
                return df 
                
            def values(self2):
                data = []
                for row in self.reader():
                    val = self.column(row, self2.col)
                    if val:
                        data.append(val)
                return data
                
            def std(self2):
                return stdev(self2.values)
                
            def median(self2):
                """ 
                Creates an estimated median by creating array of all values 
                and then using statistics.median 
                
                """
                arr = []
                med = None
                meds = []
                
                for row in self._lines():
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
                for row in self._lines():
                    if not self.filt or self.filt(row):
                        
                        val = self.column(row, self2.col)
                        try:
                            total = total + val
                            rows = rows + 1
                        except:
                            pass
                
                return total/rows
                
               
            def describe(self2):   
                arr = []
                tot = 0
                rows = 0
                meds = []
                mn = None
                mx = None
                
                for row in self._lines():
                    if not self.filt or self.filt(row):
                        val = self.column(row, self2.col)
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



                return {
                    "Median":   median(meds),
                    "Mean"  :   tot/rows,
                    #"Std":      stdev(arr),
                    "Min":      mn,
                    "Max":      mx,
                    "rows": rows,
                    #"total": tot
                    
                }
                

                
                    
            def value_counts(self2, not_pandas=False, normalize=False):
                """ Gets count of distinct values for a column """
                if normalize:
                    return self2.value_pcts(self2.col, not_pandas)
                
                vals = {}
                for row in self._lines():
                    if not self.filt or self.filt(row):
                        val = self.column(row, self2.col)
                        
                        if val not in vals: vals[val] = 0
                        
                        vals[val] = vals[val] + 1

                if not_pandas:
                    return vals
                else:
                    return pd.DataFrame(vals.items(), columns=[self2.col, "counts"])
            
            def value_pcts(self2, not_pandas=False):
                """ Value counts as a % of total number of rows """
                counts = self2.value_counts(self2.col, not_pandas=True)
                
                pcts = [(k, val/sum(counts.values())) for k, val in counts.items()]

                if not_pandas:
                    return pcts
                else:
                    return pd.DataFrame(pcts, columns=[self2.col, "pct"])
        
        for col in self.columns:
            setattr(self, col, IterativeSeries(col))
        
        self.f = self.reader()
        
    def _lines(self):
        return self.f
        
        
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
            val = row[col]
            
            if getattr(self, col)._clean:
                val = getattr(self, col).clean(val)
            if val:
                if getattr(self, col).dtype:
                    val = getattr(self, col)._astype(val)
                
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
        
        if type(cols) == str:
            cols = [cols]
            
        for col in cols:
            arrs[col] = []
            
        for row in self._lines():
            if not self.filt or self.filt(row):
                for col in cols:
                    val = self.column(row, col)
                    arrs[col].append(val)
                    
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
        return sum(1 for line in self.f if not self.filt or self.filt(line))



def read_csv(file, delimiter=",", columns=[], fwf_colmap={}, encoding=None):
    """

    delimiter: determines type of separated file, such as ",", "\t", "|" -- or "fwf" for fixed with files
    columns: for delimited file types, a list of column names in the order they appear
    fwf_colmap: for fixed width files, a dictionary that maps column name to a list of
    start and endpoints for that column {'colname': [0,2], 'colname2': [3,4]}

    """ 
    df = IterativeDF(file, delimiter=delimiter, columns=columns, fwf_colmap=fwf_colmap, encoding=encoding)
    return df



import time

if __name__ == "__main__":
    start = time.time()
    df = read_csv("bulk.csv")
    #print(df.head())
    
    df.Avg_Tot_Sbmtd_Chrgs.astype(float)
    
    df.Avg_Tot_Sbmtd_Chrgs.set_clean(lambda x: None if x == '' else x)
    #print(df.columns)
    print(df.Avg_Tot_Sbmtd_Chrgs.describe())
    end = time.time()
    print(end - start)
    
    start = time.time()
    df2 = pd.read_csv("bulk.csv")
    print(df2.Avg_Tot_Sbmtd_Chrgs.describe())
    end = time.time()
    print(end - start)
