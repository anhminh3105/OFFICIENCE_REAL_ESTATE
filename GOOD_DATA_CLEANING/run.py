#!/usr/bin/python
# -*- coding: utf-8 -*- 
import datetime
import time 
import sys
import pandas as pd
import os.path as osp
from clean_utils import *
from sql_utils import *

def main():
    usr = 'connect_python'
    pwd = '123456789'
    host = '172.16.0.184'
    db = 'REAL_ESTATE'
    tb = 'GOOD_DATA'
    some = 1000
    col_names = ['ID_CLIENT',
                'SITE',
                'ADS_DATE',
                'SALE_TYPE',
                'LAND_TYPE',
                'SQUARE',
                'FULL_ADDRESS',
                'STREET',
                'WARD',
                'DISTRICT',
                'CITY',
                'UTILITIES',
                'PRICE']

    col_to_decode = ['ADS_DATE',
                    'SALE_TYPE',
                    'LAND_TYPE',
                    'SQUARE',
                    'FULL_ADDRESS',
                    'STREET',
                    'WARD',
                    'DISTRICT',
                    'CITY',
                    'UTILITIES',
                    'PRICE']
    iter = 0                                                                                                                # Define tracker of number of iterations
    times = 1
    empty_TEMP_query = "DELETE FROM TEMP"

    reset_flag(1, usr, pwd, host, db, tb)  # SET 1 FLAG IN GOOD_DATA TO 0 TO REDO
    reset_flag(-1, usr, pwd, host, db, tb) # SET -1 FLAG IN GOOD_DATA TO 0 TO REDO
    exec_query(empty_TEMP_query, usr, pwd, host, db, 'TEMP') # tempty TEMP table
    city_config_dict = read_config_dict_from_file('city_config.csv')
    land_type_dict = read_config_dict_from_file('land_type_config.csv')

    #for _ in range(times):
    while True:
        start_time = time.time()
        
        iter += 1
        print('*********EXCECUTING BATCH NUMBER %d (WITH BATCH SIZE %d)**********' % (iter, some))
        rows = pull_rows_from_db(col_names, usr, pwd, host, db, tb, some)
        
        if not rows:
            break

        df = pd.DataFrame(rows, columns=col_names)
        df[col_to_decode] = df[col_to_decode].stack(dropna=False).str.decode('utf-8').unstack()
        
        df = clean_price(df)
        df = clean_address_new(df, city_config_dict, usr, pwd, host, db, tb)
        df = clean_land_types(df, land_type_dict, usr, pwd, host, db, tb)
        df = clean_sale_type(df)
        df = clean_square(df, usr, pwd, host, db, tb)
        df = get_unit_square(df)
        df = get_v_unit_price(df, usr, pwd, host, db, tb)
        df = get_amount(df)
        
        #df = clean_utils(df)

        '''
        filename = 'MINING_DATA.csv'
        if not osp.exists(filename): 
            df.to_csv(filename, index=False)
        else:
            df.to_csv(filename, mode='a', header=False, index=False)
        '''
        '''
        filename = 'MINING_DATA.csv'
        df.to_csv(filename, index=False)
        '''
        set_flag_rows(1, df, usr, pwd, host, db, tb)
        insert_rows_to_db(df, usr, pwd, host, db, 'TEMP')
        
        print("--- Ececution finished in %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
    

        