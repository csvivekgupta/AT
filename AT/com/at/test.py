#from com.at.app.App import App

from com.at.db.dao.DatabaseAccess import DatabaseAccess db

database = db("D:/Vivek/python/sqlitedb/ATDatabase.db")
#print(App().get_inscope_ticker())


#import sqlite3

#db = sqlite3.connect("D:/Vivek/python/sqlitedb/ATDatabase.db")
#cursor = db.execute('SELECT * FROM inscope_ticker')
#studentList = cursor.fetchall()
#print(studentList)
#print(cursor.description)

#columnNames = list(map(lambda x: x[0], cursor.description)) #students table column names list
# studentsAssoc = {} #Assoc format is dictionary similarly
# 
# 
# #THIS IS ASSOC PROCESS
# for lineNumber, student in enumerate(studentList):
#     studentsAssoc[lineNumber] = {}
# 
#     for columnNumber, value in enumerate(student):
#         studentsAssoc[lineNumber][columnNames[columnNumber]] = value
# 
# 
# #to insert into a sqlite table using dictionary
# print(studentsAssoc)
# values = {'TICKER': 'RCOM', 'TICKER_NAME': 'Relience Communications'}
# columns = ', '.join(values.keys())
# placeholders = ', '.join('?' * len(values))
# print(columns)
# print(placeholders)
# print(values.values())
# #sql = 'INSERT INTO Media ({}) VALUES ({})'.format(columns, placeholders)
# #cur.execute(sql, values.values())
# 
# #Alternative way to insert the data into table using dictionary
# 
# #Traversing dictionary
# sql_columns = 'insert into inscope_ticker('
# sql_data = ' values ('
# for column, data in values.items():
#     sql_columns+= column
#     sql_columns+= ","
#     sql_data+= "'"
#     sql_data+= data
#     sql_data+= "',"
# print(sql_columns[0:-1]+')'+sql_data[0:-1]+')')
# 
# print(sql_columns[0:-1])

