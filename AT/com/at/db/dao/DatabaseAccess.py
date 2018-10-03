'''
Created on Sep 2, 2018

@author: vivek
'''

from com.at.db.util.Database import Database as db


class DatabaseAccess(db):
    '''
    Class to interact with Database layer from Application Layer.
    It have following functionalities -
    '''

    def __init__(self, dbFileName):
        '''
        Constructor
        '''
        db.__init__(self, dbFileName)
    
    def print_records(self):
        return self.getDataInDictFormat("inscope_ticker", "TICKER, TICKER_NAME")
    
    def getActiveInscopeTickerData(self):    
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM INSCOPE_TICKER where inscope = 'Y' and sys_end_time is null")
    
    def getAllInscopeTickerData(self):    
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM INSCOPE_TICKER")
    
    def getActiveTickerTransactionData(self):
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM TICKER_TRANSACTION WHERE SYS_END_TIME IS NULL")
    
    def getAllTickerTransactionData(self):
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM TICKER_TRANSACTION")
    
    def getActiveTickerInternalTransactionData(self):
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM TICKER_INTERNAL_TRANSACTION WHERE SYS_END_TIME IS NULL")
    
    def getAllTickerInternalTransactionData(self):
        return self.getDataBasedOnQueryInDictFormat("SELECT * FROM TICKER_INTERNAL_TRANSACTION")
    
    def insertInscopeTickerData(self, dictInscopeTicker):
        tableName = 'INSCOPE_TICKER'
        insertQuery = self.prepareInsertQueryFromDict(tableName, dictInscopeTicker)
        print(insertQuery)
        self.query(insertQuery)
    
    def insertTickerTransactionData(self, dictInscopeTicker):
        tableName = 'TICKER_TRANSACTION'
        insertQuery = self.prepareInsertQueryFromDict(tableName, dictInscopeTicker)
        print(insertQuery)
        self.query(insertQuery)
        
    def insertTickerInternalTransactionData(self, dictInscopeTicker):
        tableName = 'TICKER_INTERNAL_TRANSACTION'
        insertQuery = self.prepareInsertQueryFromDict(tableName, dictInscopeTicker)
        print(insertQuery)
        self.query(insertQuery)
    
    def prepareInsertQueryFromDict(self, tableName, dictData):
        
        sqlColumns = 'insert into '
        sqlColumns += tableName
        sqlColumns += ' (SYS_START_TIME, SYS_LAST_UPD_TIME, '
        sqlData = ' values (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '
        
        for column, data in dictData.items():
            sqlColumns += column
            sqlColumns += ","
            sqlData += "'"
            sqlData += str(data)
            sqlData += "',"
            #sqlData += ","
        
        insertQuery = sqlColumns[0:-1] + ')' + sqlData[0:-1] + ')'        
        
        return insertQuery
