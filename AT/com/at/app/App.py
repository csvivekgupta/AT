'''
Created on Sep 4, 2018

@author: vivek
'''
from kiteconnect import KiteConnect
from com.at.db.dao.DatabaseAccess import DatabaseAccess as db

class App:
    '''
    Class for main logic
    '''

    def __init__(self):
        '''
        Constructor
        '''
        #self.database = db("D:/VivekLearning/ATProject/SQLiteDBFile/ATDatabase.db")
        self.database = db("D:/Vivek/python/sqlitedb/ATDatabase.db")
    
    def process(self):
        activeInscopeTicker = self.database.getActiveInscopeTickerData();
        print(activeInscopeTicker)
        activeTickerTransaction = self.database.getActiveTickerTransactionData();
        print(activeTickerTransaction)
        
        for inscopeTicker in activeInscopeTicker.values():
            print(inscopeTicker)
            tickerId = inscopeTicker["TICKER_ID"]
            if self.doesTickerIdExistInTickerTransaction(activeTickerTransaction, tickerId):
                print(tickerId , "Exists")
                
            else:
                print(tickerId , "does not exist")
                # Buy
                self.buyTicker(inscopeTicker)                
            
        #insertStmt = {'TICKER': 'INFY', 'TICKER_NAME': 'Infosys ', 'QUANTITY': 10, 'EXCHANGE': 'NSE', 'INSCOPE': 'Y', 'PROFIT_PCT': 4, 'LOSS_PCT': 1}
        #self.database.insertInscopeTickerData(insertStmt)
        self.database.close()
        
    def doesTickerIdExistInTickerTransaction(self, tickerTransactionDict, tickerId):
        tickerIdKey = "TICKER_ID"
        doesExist = False
        for tickerTransaction in tickerTransactionDict.values():
            if tickerIdKey in tickerTransaction :
                if tickerTransaction[tickerIdKey] == tickerId:
                    doesExist = True
                    break                
        return doesExist
    
    def buyTicker(self, inscopeTicker):
        #Entry in ticker_transaction
        
        tickerTransactionDict = {}
        tickerTransactionDict["TICKER_ID"] = inscopeTicker["TICKER_ID"]
        self.database.insertTickerTransactionData(tickerTransactionDict)
                
        # Read from zerodha api, what all information is required to 
        # buy a share
        print("BUY - ", inscopeTicker)
        # Entry in ticker_transaction
        
        # Entry in ticker_internal_transaction
    
if __name__ == "__main__":
    app = App()
    app.process()