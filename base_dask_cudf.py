import psutil
import dask_cudf as dc
import dask.dataframe as dd
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
import cudf
import graphviz


class BaseDfBench(object):
    def __init__(self, file_path='/data/parquet'):
        cluster = LocalCUDACluster()
        client = Client(cluster)
        client.run(cudf.set_allocator, "managed")
        self.df = dc.read_parquet(file_path, blocksize="256MB")
    
    def get_df(self):
        """
        Returns the internal dataframe
        """
        return self.df

    def done(self):
        """
        Called when the execution of the algorithm is done
        """
        pass

    def get_memory_usage(self):
        """
        Return the current memory usage of this algorithm instance
        (in kilobytes), or None if this information is not available.
        """
        # return in kB for backwards compatibility
        return psutil.Process().memory_info().rss / 1024

    '''
    def load_dataset(self, path, format, conn=None, **kwargs):
        """
        Load the provided dataframe
        :param path: path of the file to load
        :param format: format (json, csv, xml, excel, parquet, sql)
        :param kwargs: extra arguments
        :return:
        """

        if format == "csv":
            self.df = self.read_csv(path, **kwargs)
        elif format == "json":
            self.df = self.read_json(path, **kwargs)
        elif format == "xml":
            self.df = self.read_xml(path, **kwargs)
        elif format == "excel":
            self.df = self.read_excel(path, **kwargs)
        elif format == "parquet":
            self.df = self.read_parquet(path, **kwargs)
        elif format == "sql":
            self.df = self.read_sql(path, conn, **kwargs)

        return self.df

    def read_csv(self, path, **kwargs):
        """
        Read a csv file
        :param path: path of the file to load
        :param kwargs: extra arguments
        """

        self.df = cudf.read_csv(path, **kwargs)

        return self.df

    def read_json(self, path, **kwargs):
        """
        :param path: path of the file to load
        :param kwargs: extra arguments
        Read a json file
        """

        self.df = cudf.read_json(path, **kwargs)
        pass

    def read_xml(self, path, **kwargs):
        """
        Read a xml file
        :param path: path of the file to load
        :param kwargs: extra arguments
        """
        import xmltodict, json

        with open(path) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
        
        xml_file.close()

        json_data = json.dumps(data_dict)

        self.df = self.read_json(json_data)

        return self.df

    def read_excel(self, path, **kwargs):
        """
        Read an excel file
        :param path: path of the file to load
        :param kwargs: extra arguments
        """
        pass

    def read_parquet(self, path, **kwargs):
        """
        Read a parquet file
        :param path: path of the file to load
        :param kwargs: extra arguments
        """
        pass

    def read_sql(self, query, conn, **kwargs):
        """
        Given a connection and a query
        creates a dataframe from the query output
        :param query query to run to get the data
        :param conn connection to a database
        :param kwargs: extra arguments
        """
        pass
    '''
    def sort(self, column, ascending=True):
        """
        Sort the dataframe by the provided column
        :param column column to use for sorting
        :param ascending if sets to False sorts in descending order (default True)
        """

        self.df = self.df.sort_values(column, ascending=ascending)
        
        return self.df

    def get_columns(self):
        """
        Return a list containing the names of the columns in the dataframe
        """
        
        return list(self.df.columns.values)

    def is_unique(self, column):
        """
        Check the uniqueness of all values contained in the provided column_name
        :param column column to check
        """
        
        return (a[column].unique().shape[0] == a[column].shape[0]).compute()

    def delete_columns(self, columns):
        """
        Delete the provided columns
        Columns is a list of column names
        :param columns columns to delete
        """
        
        self.df = self.df.drop(columns=columns)

        return self.df

    def rename_columns(self, columns):
        """
        Rename the provided columns using the provided names
        Columns is a dictionary: {"column_name": "new_name"}
        :param columns a dictionary that contains for each column to rename the new name
        """
        
        self.df = self.df.rename(columns=columns)

        return self.df
    
    def merge_columns(self, columns, separator, name):
        """
        Create a new column with the provided name combining the two provided columns using the provided separator
        Columns is a list of two column names; separator and name are strings
        :param columns columns to merge
        :param separator separator to use
        :param name new column name
        """

        self.df[name] = self.df[columns[0]].astype(str) + separator + self.df[columns[1]].astype(str)

        return self.df

    def fill_nan(self, value=0, method=None):
        """
        Fill nan values in the dataframe with the provided value
        :param value value to use for replacing null values
        :param method method to fill the nan value, can be ffill, bfill, pad, backfill or None. Default None
        """
        
        self.df = self.df.fillna(value, method=method)
        
        return self.df
    
    def npartitions(self):
        """
        Return the number of partitions
        """
        return self.df.npartitions

    def one_hot_encoding(self, columns):
        """
        Performs one-hot encoding of the provided columns
        Columns is a list of column names
        :param columns columns to encode
        
        !!This function throw a Warning about unknown partitions but before concatenate the columns i explicitly check the number of partitions!!
        """

        dummies = dd.get_dummies(self.df.categorize(columns)[columns]).repartition(npartitions=self.df.npartitions)
        self.df = self.df.repartition(npartitions=self.df.npartitions)
        self.df = dd.multi.concat([self.df.drop(columns=columns), dummies], axis=1)
        return self.df

    def locate_null_values(self, column):
        """
        Returns the rows of the dataframe which contains
        null value in the provided column.
        :param column column to explore
        """
        
        return self.df[self.df[column].isna()]

    def substitute_by_pattern(self, column, pattern, sub):
        """
        Returns the rows of the dataframe which
        match with the provided pattern
        on the provided column.
        Pattern could be a regular expression.
        :param column column to search on
        :param pattern pattern to search, string or regex
        """
        self.df[column] = self.df[column].map_partitions(lambda d: d.str.replace(pattern, sub, regex=True))

        return self.df

    def search_by_pattern(self, column, pattern):
        """
        Returns the rows of the dataframe which
        match with the provided pattern
        on the provided column.
        Pattern could be a regular expression.
        :param column column to search on
        :param pattern pattern to search, string or regex
        """
        return self.df[column].map_partitions(lambda d: d.str.contains(pattern, regex=True))

    def locate_outliers(self, column, lower_quantile=0.1, upper_quantile=0.99):
        """
        Returns the rows of the dataframe that have values
        in the provided column lower or higher than the values
        of the lower/upper quantile.
        :param column column to search on
        :param lower_quantile lower quantile (default 0.1)
        :param upper_quantile upper quantile (default 0.99)
        """
        q_low = self.df[column].quantile(lower_quantile)
        q_hi  = self.df[column].quantile(upper_quantile)

        return self.df[(self.df[column] < q_low) | (self.df[column] > q_hi)]

    def get_columns_types(self):
        """
        Returns a dictionary with column types
        """
        
        return {k: v.name for k, v in dict(self.df.dtypes).items()}

    def cast_columns_types(self, dtypes):
        """
        Cast the data types of the provided columns
        to the provided new data types.
        dtypes is a dictionary that provides for each
        column to cast the new data type.
        :param dtypes a dictionary that provides for ech column to cast the new datatype
               For example  {'col_name': 'int8'}
        """

        self.df = self.df.astype(dtypes)

        return self.df

    def get_stats(self):
        """
        Returns dataframe statistics.
        Only for numeric columns.
        Min value, max value, average value, standard deviation, and standard quantiles.
        """
        
        return self.df.describe()

    #TODO
    #ma mi sa da cambiare proprio, purtroppo cudf non ha Dataframe.to_numeric() ma ha solo cudf.to_numeric, il ché rende impossibile la conversione per questioni di file meta
    def find_mismatched_dtypes(self):
        """
        Returns, if exists, a list of columns with mismatched data types.
        For example, a column with string dtypes that contains only integer values.
        For every columns the list contain an object with three keys:
         - Col: name of the column
         - current_dtype: current data type
         - suggested_dtype: suggested data type
        """
        
        current_dtypes = self.get_columns_types()
        #new_dtypes = self.df.(cudf.to_numeric, errors='ignore', axis=1, meta=self.df).dtypes.apply(lambda x: x.name).to_dict()

        out = []
        for k in current_dtypes.keys():
            if new_dtypes[k] != current_dtypes[k]:
                out.append({'col': k, 'current_dtype': current_dtypes[k], 'suggested_dtype': new_dtypes[k]})
        
        return out

    def check_allowed_char(self, column, pattern):
        """
        Return true if all the values of the provided column
        follow the provided pattern.
        For example, if the pattern [a-z] is provided the string
        'ciao' will return true, the string 'ciao123' will return false.
        :param column column to check
        :param pattern pattern to use
        """
        import re

        return self.df[column].str.contains(pattern, regex=True).all()

    def drop_duplicates(self, subset=[]):
        """
        Drop duplicate rows.
        """
        if subset == []:
            self.df = self.df.drop_duplicates()
        else:
            self.df = self.df.drop_duplicates(subset=subset)

        return self.df

    def drop_by_pattern(self, column, pattern):
        """
        Delete the rows where the provided pattern
        occurs in the provided column.
        """
        
        self.df = self.df[self.df.search_by_pattern(column, pattern)]

        return self.df

    #TODO
    #PRIMA DI FARE QUESTO SERVE IL CONVERTER DELLA STRNGA SCRITTA IN ITA IN DATA
    def change_date_time_format(self, column, str_date_time_format):
        """
        Change the date/time format of the provided column
        according to the provided formatting string.
        column datatype must be datetime
        An example of str_date_time_format is '%m/%d/%Y'
        :param column column to format
        :param str_date_time_format datetime formatting string
        """
        
        self.df[column] = cudf.to_datetime(self.df[column].dt.strftime(str_date_time_format))
        
        return self.df

    def set_header_case(self, case):
        """
        Put dataframe headers in the provided case
        Supported cases: "lower", "upper", "title", "capitalize", "swapcase"
        :param case case format (lower, upper, title, capitalize, swapcase)
        """
       
        if case == "lower":
            self.df.columns = map(str.lower, self.df.columns)
        elif case == "upper":
            self.df.columns = map(str.upper, self.df.columns)
        elif case == "title":
            self.df.columns = map(str.title, self.df.columns)
        elif case == "capitalize":
            self.df.columns = map(str.capitalize, self.df.columns)
        elif case == "swapcase":
            self.df.columns = map(str.swapcase, self.df.columns)

        return self.df

    def set_content_case(self, columns, case):
        """
        Put dataframe content in the provided case
        Supported cases: "lower", "upper", "title", "capitalize", "swapcase"
        (see definitions in pandas documentation)
        Columns is a list of two column names; empty list for the whole dataframe
        :param columns columns to modify
        :param case case format (lower, upper, title, capitalize, swapcase)
        """
        
        if len(columns) == 0:
            columns = list(self.df.columns.values)
        for column in columns:
            if case == "lower":
                self.df[column] = self.df[column].str.lower()
            elif case == "upper":
                self.df[column] = self.df[column].str.upper()
            elif case == "title":
                self.df[column] = self.df[column].str.title()
            elif case == "capitalize":
                self.df[column] = self.df[column].str.capitalize()
            elif case == "swapcase":
                self.df[column] = self.df[column].str.swapcase()

        return self.df

    def duplicate_columns(self, columns):
        """
        Duplicate the provided columns (add to the dataframe with "_duplicate" suffix)
        Columns is a list of column names
        :param columns columns to duplicate
        """
        
        for column in columns:
            self.df[column + "_duplicate"] = self.df[column]
        
        return self.df

    #FIXME QUESTA VA CONTROLLATA!!! Applicare la aggfunc così non è legale per Rapids
    def pivot(self, index, columns, values, aggfunc):
        """
        Define the lists of columns to be used as index, columns and values respectively,
        and the dictionary to aggregate ("sum", "mean", "count") the values for each column: {"col1": "sum"}
        :param index Column to use to make new frame’s index. If None, uses existing index.
        :param columns Column to use to make new frame’s columns.
        :param values  Column(s) to use for populating new frame’s values.
        :param aggfunc dictionary to aggregate ("sum", "mean", "count") the values for each column
               {"col1": "sum"}
        """
        
        self.df = self.df.pivot(index=index, values=values, columns=columns, aggfunc=aggfunc).reset_index()
        
        return self.df

    #FIXME ANCHE QUESTA VA CONTROLLATA
    def unpivot(self, columns, var_name, val_name):
        """
        Define the list of columns to be used as values for the variable column,
        the name for variable columns and the one for value column_name
        """
        
        self.df = self.df.melt(id_vars=list(set(list(self.df.columns.values)) - set(columns)), value_vars=columns, var_name=var_name, value_name=val_name)
        
        return self.df

    def delete_empty_rows(self, columns):
        """
        Delete the rows with null values for all provided Columns
        Columns is a list of column names
        :param columns columns to check
        """
        self.df = self.df.dropna(subset = columns, how='all')
        
        return self.df

    def split(self, column, splits, sep=None):
        """
        Split the provided column into splits + 1 columns named after col_names
        using the provided sep string as separator
        Col_names is a list of column names
        :param column column to split
        :param sep separator
        :param splits number of splits, limit the number of splits
        :param col_names name of the new columns
        """
        
        return self.df[column].str.split(sep, splits, expand=True)

    def strip(self, columns, chars):
        """
        Remove the characters appearing in chars at the beginning/end of the provided columns
        Columns is a list of column names
        :param columns columns to edit
        :param chars characters to remove
        """
        
        for column in columns:
            self.df[column] = self.df[column].str.strip(chars)

        return self.df

    def remove_diacritics(self, columns):
        """
        Remove diacritics from the provided columns
        Columns is a list of column names
        :param columns columns to edit
        """
        
        for column in columns:
            self.df[column] = self.df[column].str.normalize_characters('NFKD')
        
        return self.df

    def set_index(self, column):
        """
        Set the provided column as index
        :param column to use as index
        """
        
        self.df = self.df.set_index(column)
        
        return self.df

    def change_num_format(self, formats):
        """
        Round one ore more columns to a variable number of decimal places.
        formats is a dictionary with the column names as key and the number of decimal places as value.
        :param formats new column(s) format(s).
               E.g. {'col_name' : 2}
        """
        
        self.df = self.df.round(formats)
        
        return self.df

    def calc_column(self, col_name, f):
        """
        Calculate the new column col_name by applying
        the function f.
        :param col_name column on which apply the function
        :param f function to apply
        
        EXAMPLE: df.calc_column('BBBBBBBBBBBB', lambda x: x['gas_transport_cost']+99).tail()
        """
        
        self.df[col_name] = self.df.apply(f, axis=1)
        
        return self.df

    def join(self, other, left_on=None, right_on=None, how='inner', **kwargs):
        """
        Joins current dataframe (left) with a new one (right).
        left_on/right_on are the keys on which perform the equijoin
        how is the type of join
        **kwargs: additional parameters
        The result is stored in the current dataframe.
        :param other dataframe to join
        :param left_on key of the current dataframe to use for join
        :param right_on key of the other dataframe to use for join
        :param how type of join (inner, left, right, outer)
        :param kwargs extra parameters
        """
        
        self.df = self.df.merge(other, left_on=left_on, right_on=right_on, how=how, **kwargs)
        
        return self.df

    def groupby(self, columns, f):
        """
        Aggregate the dataframe by the provided columns
        then applies the function f on every group
        :param columns columns to use for group by
        :param f aggregation function
        """
        
        return self.df.groupby(columns).agg(f)

    def categorical_encoding(self, columns):
        """
        See label encoding / ordinal encoding by sklearn
        Convert the categorical values in these columns into numerical values
        Columns is a list of column names
        :param columns columns to encode
        """
        self.df = self.df.categorize(columns=columns)
        for column in columns:
            self.df[column] = self.df[column].cat.codes
        
        return self.df

    def sample_rows(self, num=0.01):
        """
        NB. DASK NON SUPPORTA UN NUMERO PREDEFINITO DI RIGHE MA SOLAMENTE UNA FRAZIONE
        Return a sample of the rows of the dataframe
        Frac is a boolean:
        - if true, num is the percentage of rows to be returned
        - if false, num is the exact number of rows to be returned
        :param frac percentage or exact number of samples to take
        :param num if set to True uses frac as a percentage, otherwise frac is used as a number
        """
        
        assert 0 <= num <= 1, 'num MUST BE between 0 and 1'
        return self.df.sample(frac=num)

    def append(self, other, axis=0):
        """
        Append the rows of another dataframe (other) at the end of the provided dataframe
        All columns are kept, eventually filled by nan
        Ignore index is a boolean: if true, reset row indices
        :param other other dataframe to append
        :param ignore_index if set to True reset row indices
        """
        
        self.df = dd.multi.concat([self.df, other], axis=axis)
        
        return self.df
######################################################################
    def replace(self, columns, to_replace, value, regex):
        """
        Replace all occurrences of to_replace (numeric, string, regex, list, dict) in the provided columns using the
        provided value
        Regex is a boolean: if true, to_replace is interpreted as a regex
        Columns is a list of column names
        :param columns columns on which apply the method
        :param to_replace value to search (could be a regex)
        :param value value to replace with
        :param regex if True means that to_replace is a regex
        """
        
        self.df[columns] = self.df[columns].replace(to_replace=to_replace, value=value, regex=regex)
        
        return self.df

    def edit(self, columns, func):
        """
        Edit the values of the cells in the provided columns using the provided expression
        Columns is a list of column names
        :param columns columns on which apply this method
        :param func function to apply
        """
        
        self.df[columns] = self.df[columns].apply(func)
        
        return self.df

    def set_value(self, index, column, value):
        """
        Set the cell identified by index and column to the provided value
        :param index row indices
        :param column column name
        :param value value to set
        """
        
        self.df.at[index, column] = value
        
        return self.df

    def min_max_scaling(self, columns, min, max):
        """
        Independently scale the values in each provided column in the range (min, max)
        Columns is a list of column names
        :param columns columns on which apply this method
        :param min min value
        :param max max value
        """
        
        for column in columns:
            self.df[column] = self.df[column] - self.df[column].min()
            self.df[column] = self.df[column] / self.df[column].max()
            self.df[column] = self.df[column] * (max - min) + min
        
        return self.df

    def round(self, columns, n):
        """
        Round the values in columns using n decimal places
        Columns is a list of column names
        :param columns columns on which apply this method
        :param n decimal places
        """
        self.df[columns] = self.df[columns].round(n)
        
        return self.df

    def get_duplicate_columns(self):
        """
        Return a list of duplicate columns, if exists.
        Duplicate columns are those which have same values for each row.
        """
        
        cols = self.df.columns.values
        
        return [(cols[i], cols[j]) for i in range(0, len(cols)) for j in range(i+1, len(cols)) if self.df[cols[i]].equals(self.df[cols[j]])]

    def to_csv(self, path, **kwargs):
        """
        Export the dataframe in a csv file.
        :param path path on which store the csv
        :param kwargs extra parameters
        """
        
        self.df.to_csv(path, **kwargs)

        pass

    def query(self, query):
        """
        Queries the dataframe and returns the corresponding
        result set.
        :param query: a string with the query conditions, e.g. "col1 > 1 & col2 < 10"
        :return: subset of the dataframe that correspond to the selection conditions
        """
        
        return self.df.query(query)