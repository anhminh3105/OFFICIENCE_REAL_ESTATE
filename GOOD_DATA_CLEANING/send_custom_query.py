#!/usr/bin/python
# -*- coding: utf-8 -*- 
from sql_utils import exec_query

def main():
    usr = 'connect_python'
    pwd = '123456789'
    host = '172.16.0.184'
    db = 'REAL_ESTATE'
    tb = 'GOOD_DATA'
    query = "UPDATE GOOD_DATA SET FLAG = 0 WHERE FLAG = 1 AND ID_CLIENT NOT IN (SELECT ID_CLIENT FROM TEMP)"

    exec_query(query, usr, pwd, host, db, tb)

if __name__ == '__main__':
    main()
    
'''
SOME QUERIES:
DELETE FROM TEMP WHERE ID_CLIENT NOT IN (SELECT ID_CLIENT FROM GOOD_DATA WHERE FLAG=1) -------- DELETE ENTRIES ACCIDENTLY ADDED TO TEMP BUT HAVEN'T BEEN UPDATED IN GOOD_DATA
UPDATE GOOD_DATA SET FLAG = 0 WHERE FLAG = 1 AND ID_CLIENT NOT IN (SELECT ID_CLIENT FROM TEMP)
SELECT ID_CLIENT FROM GOOD_DATA WHERE ID_CLIENT NOT IN (SELECT ID_CLIENT FROM `TEMP`) AND FLAG=1 
'''