#!/usr/bin/python
# -*- coding: utf-8 -*-

import mysql.connector
from sqlalchemy import create_engine

def exec_query(query, usr, pwd, host, db, tb):
    cnx = mysql.connector.connect(user=usr, password=pwd, host=host, database=db , charset='utf8')
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()


def reset_flag(value_to_be_reset, usr, pwd, host, db, tb):
    query = "UPDATE " + db + "." + tb + " SET FLAG=0 WHERE FLAG=" + str(value_to_be_reset)
    exec_query(query, usr, pwd, host, db, tb)
      
def set_flag_rows(value, df, usr, pwd, host, db, tb):
    for _, row in df.iterrows():
        set_flag_query = "UPDATE " + db + "." + tb + " SET FLAG = " + str(value) + " WHERE SITE='" + "{}".format(row.SITE) + "' AND ID_CLIENT='" + row.ID_CLIENT + "' AND FLAG=0"
        exec_query(set_flag_query, usr, pwd, host, db, tb)

def insert_rows_to_db(df, usr, pwd, host, db, tb_to_insert):
    url = "mysql://" + usr + ":" + pwd + "@" + host + "/" + db + "?charset=utf8"
    engine = create_engine(url)
    df.to_sql(tb_to_insert, con=engine, if_exists='append', index=False)

def pull_rows_from_db(col_to_retrieve_list, usr, pwd, host, db, tb, limit):

    col_names_str = str(col_to_retrieve_list).replace("]","")
    col_names_str = col_names_str.replace("[","")
    col_names_str = col_names_str.replace("'","")

    query = "SELECT " + str(col_names_str) + " FROM " + db + "." + tb + " WHERE FLAG = 0 AND SITE != 'RONGBAY' LIMIT " + str(limit)                  # Construct query
    #query = "SELECT " + str(col_names_str) + " FROM " + db + "." + tb + " WHERE FULL_ADDRESS LIKE " + "'%Huyện%'" + " AND DISTRICT LIKE " + "'%Huyện%'" + " LIMIT " + str(some)   # testing query
    #fetch = cursor.fetchmany                                                                                                # Fetch rows in batches
    #rows = fetch(some)

    cnx =mysql.connector.connect(user=usr, password=pwd, host=host, database=db , charset='utf8')                           # Define mysql connector
    cursor = cnx.cursor()
    cursor.execute(query)

    rows = cursor.fetchall()

    cnx.commit()
    cursor.close()
    cnx.close()

    return rows