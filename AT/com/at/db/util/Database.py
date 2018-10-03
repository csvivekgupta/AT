'''
Created on Aug 29, 2018

@author: vivek
'''

###########################################################################
#
# # @file database.py
#
###########################################################################

import sqlite3


class Database:
    '''
    The Database class is a high-level wrapper around the sqlite3
    library. It allows users to create a database connection and
    write to or fetch data from the selected database. It also has
    various utility functions such as getLast(), which retrieves
    only the very last item in the database, toCSV(), which writes
    entries from a database to a CSV file, and summary(), a function
    that takes a dataset and returns only the maximum, minimum and
    average for each column. The Database can be opened either by passing
    on the name of the sqlite database in the constructor, or optionally
    after constructing the database without a name first, the open()
    method can be used. Additionally, the Database can be opened as a
    context method, using a 'with .. as' statement. The latter takes
    care of closing the database.
    '''

    def __init__(self, name=None):
        '''
        The constructor can either be passed the name of the database to open
        or not, it is optional. The database can also be opened manually with
        the open() method or as a context manager.
    
        @param name Optionally, the name of the database to open.
    
        @see open()
        '''
        
        self.conn = None
        self.cursor = None

        if name:
            self.open(name)
        
    def open(self, name):
        '''
        This function manually opens a new database connection. The database
        can also be opened in the constructor or as a context manager. 
           
        @param name The name of the database to open.    
        
        @see \__init\__()
        '''
        
        try:
            self.conn = sqlite3.connect(name);
            self.cursor = self.conn.cursor()

        except sqlite3.Error as e:
            print("Error connecting to database! ", e)

    def close(self):        
        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def __enter__(self):        
        return self

    def __exit__(self, exc_type, exc_value, traceback):        
        self.close()
        
    def get(self, table, columns, limit=None):
        '''
        This is the main function used to query a database for data. 
           
        @param table The name of the database's table to query from.  
          
        @param columns The string of columns, comma-separated, to fetch.  
          
        @param limit Optionally, a limit of items to fetch.
        
        '''

        query = "SELECT {0} from {1};".format(columns, table)
        self.cursor.execute(query)

        # fetch data
        rows = self.cursor.fetchall()

        return rows[len(rows) - limit if limit else 0:]
        
    def getDataInDictFormat(self, table, columns):
        
        query = "SELECT {0} from {1};".format(columns, table)
        self.cursor.execute(query)

        # fetch data
        rows = self.cursor.fetchall()
        #print(rows)
        #print(self.cursor.description)
        
        columnNames = list(map(lambda x: x[0], self.cursor.description)) #table column names list
        rowsAssoc = {} #Assoc format is dictionary similarly


        #THIS IS ASSOC PROCESS
        for lineNumber, table in enumerate(rows):
            rowsAssoc[lineNumber] = {}

            for columnNumber, value in enumerate(table):
                rowsAssoc[lineNumber][columnNames[columnNumber]] = value


        return rowsAssoc
        
    def getDataBasedOnQueryInDictFormat(self, query):
        
        self.cursor.execute(query)

        # fetch data
        rows = self.cursor.fetchall()
        #print(rows)
        #print(self.cursor.description)
        
        columnNames = list(map(lambda x: x[0], self.cursor.description)) #table column names list
        rowsAssoc = {} #Assoc format is dictionary similarly


        #THIS IS ASSOC PROCESS
        for lineNumber, table in enumerate(rows):
            rowsAssoc[lineNumber] = {}

            for columnNumber, value in enumerate(table):
                rowsAssoc[lineNumber][columnNames[columnNumber]] = value


        return rowsAssoc
    
    
    def getLast(self, table, columns): 
        '''
        Utility function to get the last row of data from a database.
    
        @param table The database's table from which to query.
    
        @param columns The columns which to query.
        '''       
        return self.get(table, columns, limit=1)[0]

    @staticmethod
    def toCSV(data, fname="output.csv"):
        '''
        Utility function that converts a data set into CSV format.
    
        @param data The data, retrieved from the get() function.
    
        @param fname The file name to store the data in.
    
        @see get()
        '''
        with open(fname, 'a') as file:
            file.write(",".join([str(j) for i in data for j in i]))
            
    def write(self, table, columns, data):
        '''
        Function to write data to the database.
    
        The write() function inserts new data into a table of the database.
    
        @param table The name of the database's table to write to.
    
        @param columns The columns to insert into, as a comma-separated string.
    
        @param data The new data to insert, as a comma-separated string.
        
        '''
        query = "INSERT INTO {0} ({1}) VALUES ({2});".format(table, columns, data)

        self.cursor.execute(query)
        
    def query(self, sql):
        '''
        Function to query any other SQL statement.
    
        This function is there in case you want to execute any other sql
        statement other than a write or get.
    
        @param sql A valid SQL statement in string format.
        
        '''
        
        self.cursor.execute(sql)

    @staticmethod
    def summary(rows):
        '''
        Utility function that summarizes a dataset.
    
        This function takes a dataset, retrieved via the get() function, and
        returns only the maximum, minimum and average for each column.
    
        @param rows The retrieved data.
        
        '''
            
        # split the rows into columns
        cols = [ [r[c] for r in rows] for c in range(len(rows[0])) ]
        
        # the time in terms of fractions of hours of how long ago
        # the sample was assumes the sampling period is 10 minutes
        t = lambda col: "{:.1f}".format((len(rows) - col) / 6.0)

        # return a tuple, consisting of tuples of the maximum,
        # the minimum and the average for each column and their
        # respective time (how long ago, in fractions of hours)
        # average has no time, of course
        ret = []

        for c in cols:
            hi = max(c)
            hi_t = t(c.index(hi))

            lo = min(c)
            lo_t = t(c.index(lo))

            avg = sum(c) / len(rows)

            ret.append(((hi, hi_t), (lo, lo_t), avg))

        return ret
