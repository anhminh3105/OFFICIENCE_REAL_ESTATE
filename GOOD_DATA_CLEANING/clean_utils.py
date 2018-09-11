#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import pandas as pd
import csv
import logging
import time
import re
from datetime import datetime
from sql_utils import *

def import_temp_data(good_data='GOOD_DATA.csv'):
    raw_df = pd.read_csv(good_data, sep=',', engine='python')            # read input csv file
    
    return raw_df[[ 'ID_CLIENT',
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
                    'PRICE']]

def clean_sale_type(df):                        
    # clean SALE_TYPE
    df.SALE_TYPE = df.SALE_TYPE.str.replace('Cho thuê', 'Thuê')
    df.SALE_TYPE = df.SALE_TYPE.str.replace('chothue', 'Thuê')
    df.SALE_TYPE = df.SALE_TYPE.str.replace('canban', 'Bán')
    return df

def clean_utils(df):
    df.UTILITIES = df.UTILITIES.apply(lambda x: str(x))
    df_utilities = df.UTILITIES.str.join('').str.get_dummies(',')                    #one hot each of the utility in UTILITIES into separate columns
    df = pd.concat([df, df_utilities], axis=1).drop(['UTILITIES'], axis=1)           #merge 2 dataframes together and drop the UTILITIES column
    return df

def clean_price(df):
    #price_df = df.pop('PRICE')
    #df['PRICE'] = price_df
    set_to_zero_l = ['giá bảo mật',
                     'thỏa thuận',
                     'thương lượng',
                     'Thương lượng',
                     'thương lượng/m2',
                     'thương lượng/tháng',
                     'thỏa thuận/m²',
                     'thỏa thuận/tháng']
    #df.PRICE = df.PRICE.apply(lambda x: 0 if x in set_to_zero_l)
    
    df.PRICE = df.PRICE.str.lower()
    df.PRICE = df.PRICE.str.replace(',', '.')
    df.PRICE = df.PRICE.str.replace('/tháng', '')
    df.PRICE = df.PRICE.str.replace('/tháng', '')

    for row in range(df.shape[0]):
        if df.iloc[row, -1] in set_to_zero_l:
            df.ix[row, -1] = str(0)


    by_l = df.PRICE.apply(lambda x: x.split('/'))
    
    l = by_l.apply(lambda x: x[0].strip())
    
    l_by = by_l.apply(lambda x: try_set_unit(x))
    l = l.apply(lambda x: re.sub(r'(?<=[0-9])(?=[a-zA-Z])', r' ', x))
    l = l.apply(lambda x: x.split(' '))
    #print(l)
    df.PRICE = l.apply(lambda x: convert_price(x))
    
    df['UNIT_PRICE'] = l.apply(lambda x: try_set_unit(x))

    for row in range(df.UNIT_PRICE.shape[0]):
        if l_by[row] is not '':
            df.ix[row, -1] = df.iloc[row, -1] + '/' + l_by[row]


        current_unit = df.loc[row, "UNIT_PRICE"]
        
        if "th��ng" in current_unit:
            current_unit = current_unit.replace("th��ng", "")
        if "m²" in current_unit:
            current_unit = current_unit.replace("m²", "m2")        
        if "tri���u" in current_unit:
            current_unit = current_unit.replace("tri���u", "triệu")
        if "t��" in current_unit:
            current_unit = current_unit.replace("t��", "tỷ")
        if "ngàn" in current_unit:
            current_unit = current_unit.replace("ngàn", "nghìn")        
        if "m��" in current_unit:
            current_unit = current_unit.replace("m��", "m2")
        if current_unit == "/m2":
            current_unit = "nghìn" + current_unit
        if "tháng" in current_unit and df.at[row, 'SALE_TYPE'] == "Bán":
            current_unit = current_unit.replace("/tháng", "")
        #if current_unit == "/tháng":
        #    current_unit = current_unit.replace("/tháng", "nghìn")
        #if current_unit == "tỷ/m2":
        #    current_unit = current_unit.replace("tỷ/m2", "tỷ")
        df.iat[row, -1] = current_unit

    return df

def try_set_unit(x):
    try:
        if 'cây' in x[1]:
            return ' '.join(x[1:])
        else:
            return x[1]
    except:
        return ''

def convert_price(price_as_list):
    if 'tỷ' in price_as_list and 'triệu' in price_as_list:
        inx1 = price_as_list.index('tỷ') - 1
        inx2 = price_as_list.index('triệu') - 1
        price = float(price_as_list[inx1] + '.' + price_as_list[inx2])
        if 'ngàn' in price_as_list:
            inx3 = price_as_list.index('ngàn') - 1
            ngan_by_ty = float(str(0) + '.0' +  price_as_list[inx3])
            return price + ngan_by_ty
        return price
    elif 'triệu' in price_as_list and ('ngàn' in price_as_list or 'ngàn/m2' in price_as_list or 'ngàn/tháng' in price_as_list):
        inx1 = price_as_list.index('triệu') - 1
        if 'ngàn' in price_as_list:
            inx2 = price_as_list.index('ngàn') - 1      
        elif 'ngàn/m2' in price_as_list:
            inx2 = price_as_list.index('ngàn/m2') - 1   
        else:
            inx2 = price_as_list.index('ngàn/tháng') - 1
        return float(price_as_list[inx1] + '.' + price_as_list[inx2])
    
    else:
        #price = price_as_list[0].str.replace(',', '.')
        try:
            return float(price_as_list[0])
        except:
            return 0


def UNUSED_clean_address(raw_df):
    
    df = raw_df[['FULL_ADDRESS', 'STREET', 'WARD', 'DISTRICT', 'CITY']]
    truth_df = df.isnull()
    #print(truth_df)

    FULL_ADDRESS_inx = df.columns.get_loc('FULL_ADDRESS')
    CITY_inx = df.columns.get_loc('CITY')
    ADDRESS_inx = df.columns.get_loc('STREET')

    for row in range(df.shape[0]):                                                                      # loop over each entry
        if truth_df.iloc[row, FULL_ADDRESS_inx]:
            continue
        else:
            if ',' in df.iloc[row, FULL_ADDRESS_inx]:                                                   # split FULL_ADDRESS to list
                add_list = df.iloc[row, FULL_ADDRESS_inx].split(',')  
                #print(add_list)
            else:
                add_list = df.iloc[row, FULL_ADDRESS_inx].split('-')

            for col in range(CITY_inx, ADDRESS_inx - 1, -1):                                                # loop from CITY to street  

                if truth_df.iloc[row, col]:                                                                 # check the NULL value
                    col_from_right = col - (CITY_inx + 1)                                                   # get index
                    #print(index)
                    index = len(add_list) + col_from_right
                    if col_from_right == -4 and index > 1:
                        df.ix[row, col] = ','.join(add_list[0:(index + 1)])                                   # if list has more than 4 strings, join the remaining on the left and assign to street column
                    else:
                        try:
                            df.ix[row, col] = add_list[index]                                               # assign the right value in list to the coressponding column
                        except:
                            df.ix[row, col] = ""
                            #print(','.join(add_list[0:index+1]))
                            #print(add_list[index])
                            
                            #if col == -4 and index > 1:
                            #print(add_list[0:index+1])

    df.WARD = df.WARD.str.replace("Phường", "")                                                         # strip "phường" in WARD
    df.WARD = df.WARD.str.replace("phường", "")                                                         # strip "phường" in WARD
    df.DISTRICT = df.DISTRICT.str.replace("Quận", "")                                                   # strip "quận" in DISTRICT
    df.DISTRICT = df.DISTRICT.str.replace("quận", "")                                                   # strip "quận" in DISTRICT
    df.CITY = df.CITY.str.replace("Hồ Chí Minh", "TP.HCM")                                              # replace "Hồ Chí Minh" to "TP.HCM"

    df[df.columns] = df.apply(lambda x: x.str.strip())                                                  # strip whitespaces
    print(df.head(20))
    raw_df = raw_df.drop(['FULL_ADDRESS', 'STREET', 'WARD', 'DISTRICT', 'CITY'], axis=1)

    return pd.concat([raw_df, df], axis=1)




def remove_duplicate_in_string(string):
    str_list = string.split(" ")
    u_list = []
    [u_list.append(s) for s in str_list if s not in u_list]
    return " ".join(u_list)

def full_address_to_list(full_address):
    if "," in full_address:
        full_address_list = full_address.split(",")
    else:
        full_address_list = full_address.split("-")
                
    full_address_list = [xtr.strip() for xtr in full_address_list]

    return full_address_list

def set_error_flag_to_db_and_log_message(row, mess, usr, pwd, host, db, tb):
    # CONTRUCT A DF OF 1 ROW
    #print(raw_df.loc[row, :])
    row_df_to_set = pd.DataFrame(row).T
    #print(row_df_to_set)
    set_flag_rows(-1, row_df_to_set, usr, pwd, host, db, tb)
    # LOG ROWS THAT FAILS TO UPDATE
    logging.basicConfig(filename="log.log", level=logging.DEBUG)
    template = ("\n{}~Error reported at keys (ID_CLIENT: {} - SITE: {}) with message: \"{}\"")
    #log = datetime.now() + " Error reported at keys (ID_CLIENT: " + row_df_to_set.ID_CLIENT + " - SITE: " + row_df_to_set.SITE + ") with message: \"" + mess + "\""
    log = template.format(datetime.now(), row_df_to_set.ID_CLIENT, row_df_to_set.SITE, mess)
    print(log)
    logging.debug(log)

def read_config_dict_from_file(filename):
    with open(filename, mode='r') as file:
        reader = csv.reader(file)
        config_dict = {rows[0]:rows[1] for rows in reader}
    return config_dict

def clean_address_new(raw_df, city_config_dict, usr, pwd, host, db, tb):
    return_df = pd.DataFrame(columns=raw_df.columns)
    processing_cols = ['FULL_ADDRESS', 'STREET', 'WARD', 'DISTRICT', 'CITY']
    FULL_ADDRESS_inx = raw_df.columns.get_loc('FULL_ADDRESS')
    CITY_inx = raw_df.columns.get_loc('CITY')
    DISTRICT_inx = raw_df.columns.get_loc('DISTRICT')
    STREET_inx = raw_df.columns.get_loc('STREET')
    WARD_inx = raw_df.columns.get_loc('WARD')
 
    truth_df = raw_df[processing_cols].isnull()
    is_null = truth_df.any(axis=1)
    
    raw_df.iloc[:, DISTRICT_inx] = raw_df.iloc[:, DISTRICT_inx].str.replace("Thành phố", "")
    length_df = raw_df.shape[0]
  
    for row in range(length_df):
        try:
            city = raw_df.iat[row, CITY_inx]
        except KeyError:
            continue
        value_for_key = city_config_dict.get(city)
        if value_for_key is None:
            mess = "Error code 001: IN COLUMN CITY ~ VALUE: (" + str(city) + ") - VALUE IN CONFIG MATRIX: (" + str(value_for_key) + ")"
            set_error_flag_to_db_and_log_message(raw_df.loc[row, :], mess, usr, pwd,host, db, tb)
            continue
        else:
            if is_null[row]:
                null_full_address = truth_df.loc[row, 'FULL_ADDRESS']
                if not null_full_address:
                    
                    full_address = raw_df.iat[row, FULL_ADDRESS_inx]
                    full_address = full_address.replace("Hà Nội", "")
                    full_address = full_address.replace("Bán nhà riêng tại ", "")
                    full_address = full_address.replace("-- Đường / Phố --", "")
                    
                    district = raw_df.iat[row, DISTRICT_inx]
                    if "Quận" not in district and "Huyện" not in district and "Quận" in full_address:
                        district = "Quận " + district
                    elif "Quận" not in district and "Huyện" not in district and "Huyện" in full_address:
                        district = "Huyện " + district
                    elif "Quận" in district and "Huyện" not in district and "Quận" not in full_address and "Huyện" not in full_address:
                        district = district.replace("Quận ", "")
                    elif "Huyện" in district and "Quận" not in district and "Quận" not in full_address and "Huyện" not in full_address:
                        district = district.replace("Huyện ", "")
                    
                    full_address = full_address.replace(district, "")
  
                    full_address = full_address.replace(city, "")

                    null_ward = truth_df.loc[row, 'WARD']
                    if not null_ward:
                        ward = raw_df.iat[row, WARD_inx]
                        full_address = full_address.replace(str(ward), "")
                        # split the remaining of FULL_ADDRESS to list              
                        full_address_list = full_address_to_list(full_address)
                        raw_df.iat[row, STREET_inx] = " ".join(full_address_list)
                    else:
                        # split the remaining of FULL_ADDRESS to list              
                        full_address_list = full_address_to_list(full_address)

                        if full_address_list:
                            matchers = ["Phường", "phường", "Xã"]
                            ward_matching = [s for s in full_address_list if any(xs in s for xs in matchers)]

                            if ward_matching:
                                #print(full_address_list[1])
                                full_address_list.remove(ward_matching[0])
                                ward = remove_duplicate_in_string(ward_matching[0])
                                ward = ward.title()     # capitalise first letter of every word in ward
                                raw_df.iat[row, WARD_inx] = ward
                            
                            street = " ".join(full_address_list)

                            street = street.replace("Thành phố", "")
                            street = street.replace("Hà Nội", "")
                            street = street.replace("Hồ Chí Minh", "")
                            street = street.replace("VP NHÀ ĐẤT THIÊN TRƯỜNG -", "")
                            street = street.replace(" Thuộc dự án:", "")
                            street = street.replace("Dự án -- Chọn Dự án --, ", "")
                            street = street.replace("Vision", "")
                            street = street.replace("TP", "")
                            street = street.replace("TP HCM", "")
                            street = street.replace("tphcm", "")
                            street = street.replace("hà nội", "")
                            street = street.replace("HÀ NỘI", "")
                            street = street.replace("Hanoi", "")
                            raw_df.iat[row, STREET_inx] = street.replace(".", "").strip()
          
            raw_df.iat[row, CITY_inx] = value_for_key
            raw_df.loc[row, processing_cols] = raw_df.loc[row, processing_cols].str.strip()
            return_df = return_df.append(raw_df.iloc[row, :])
    return return_df

def UNUSED_clean_land_types(raw_df):
    
    BIET_THU_LIST = ["Bán nhà biệt thự, liền kề"
                    ,"ban-biet-thu-trong-du-an"
                    ,"Biệt thự đơn lập"
                    ,"Biệt thự liền kề"
                    ,"Biệt thự liềnkề"
                    ,"Biệt thự nghỉ dưỡng"
                    ,"Biệt thự nghỉ dư��ng"
                    ,"Biệt thự song lập"
                    ,"Biệt th�� liền kề"
                    ,"cho-thue-biet-thu"
                    ,"Villa - Biệt thự"]

    CHUNG_CU_LIST = ["Bán căn hộ chung cư"
                    ,"ban-can-ho-chung-cu"
                    ,"Căn hộ cao cấp"
                    ,"Căn hộ chung cư"
                    ,"Căn hộ Condotel"
                    ,"Căn hộ dich vụ"
                    ,"Căn hộ mini"
                    ,"Căn hộ Officetel"
                    ,"Căn hộ Penthouse"
                    ,"Căn hộ rẻ"
                    ,"Căn hộ Tập thể"
                    ,"Căn hộchung cư"
                    ,"Cho thuê căn hộ chung cư"
                    ,"cho-thue-can-ho-chung-cu"]

    CUA_HANG_MAT_BANG_LIST =    ["ban-mat-bang-cua-hang"
                                ,"Cho thuê cửa hàng, ki ốt"
                                ,"cho-thue-cua-hang-kiot"
                                ,"Cửa hàng kiot"
                                ,"GROUND"
                                ,"Mặt bằng - Cửa hàng"
                                ,"Mặt bằng bán lẻ"]      

    DAT_CONG_NONG_NGHIEP_LIST = ["Đất cho sản xuất"
                                ,"Đất công nghiệp"
                                ,"Đất lâm nghiệp"
                                ,"Đất nông lâm nghiệp"
                                ,"Đất nông nghiệp"
                                ,"Đất trang trại"
                                ,"Đất vườn"]

    DAT_NEN_LIST =  ["Bán đất"
                    ,"Bán đất nền dự án"
                    ,"ban-dat"
                    ,"ban-dat-nen-du-an"
                    ,"cho-thue-dat"
                    ,"Đất dự án - Quy hoạch"
                    ,"Đất nền"
                    ,"Đất nền khu dân cư"
                    ,"Đất n���n"
                    ,"Đất ở - Đất thổ cư"
                    ,"Đấtnền"
                    ,"Đ��t nền"
                    ,"Đ��t nền khu dân cư"
                    ,"Đ���t nền"
                    ,"LAND"
                    ,"��ất nền"]

    KHACH_SAN_LIST =    ["ban-khach-san-nha-nghi"
                        ,"cho-thue-khach-san-nha-nghi"
                        ,"Khách sạn"
                        ,"Khách Sạn - Nhà Phố"
                        ,"Nhà hàng - Khách sạn"]

    KHO_XUONG_LIST =    ["Bán kho, nhà xưởng"
                        ,"ban-kho-nha-xuong"
                        ,"Cho thuê kho, nhà xưởng, đất"
                        ,"cho-thue-kho-nha-xuong"
                        ,"Nhà Kho - Xưởng"
                        ,"Nhà xưởng"
                        ,"Nhà xưởng kho bãi"
                        ,"WAREHOUSE"]

    NHA_PHO_LIST =  ["Bán nhà mặt phố"
                    ,"Bán nhà mặt tiền"
                    ,"Bán nhà riêng"
                    ,"ban-nha-mat-pho"
                    ,"ban-nha-phan-lo"
                    ,"ban-nha-rieng"
                    ,"Cho thuê nhà mặt phố"
                    ,"Cho thuê nhà mặt tiền"
                    ,"Cho thuê nhà riêng"
                    ,"cho-thue-nha-mat-pho"
                    ,"cho-thue-nha-rieng"
                    ,"HOUSE"
                    ,"Nhà mặt phố"
                    ,"Nhà phố"
                    ,"Nhà phố Shophouse"
                    ,"Nhà rẻ"
                    ,"Nhà riêng"
                    ,"Nhà ri��ng"
                    ,"Nhà tạm"
                    ,"Nh�� riêng"]                        

    PHONG_TRO_LIST =    ["Cho thuê nhà trọ, phòng trọ"
                        ,"cho-thue-nha-tro"
                        ,"Nhà trọ, phòng trọ"
                        ,"Nhà trọ, phòngtrọ"
                        ,"Phòng trọ"]

    RESORT_LIST =   ["Bán trang trại, khu nghỉ dưỡng"
                    ,"ban-trang-trai-khu-nghi-duong"
                    ,"Đất khu du lịch"
                    ,"Đất nghỉ dưỡng"]    
                    
    VAN_PHONG_LIST =    ["Cho thuê văn phòng"
                        ,"cho-thue-van-phong"
                        ,"TT Thương mại"
                        ,"Văn phòng"]           

    KHAC_LIST = ["Bán loại bất động sản khác"
                ,"Bất động sản khác"
                ,"Cho thuê loại bất động sản khác"
                ,"cho-thue-loai-bat-dong-san-khac"]
    for row in range(length_df):
        land_type_row = land_type_col[row]
        
        if land_type_row in BIET_THU_LIST:
            land_type_col[row] = "Biệt thự"
        
        elif land_type_row in CHUNG_CU_LIST:
            land_type_col[row] = "Chung Cư"
        
        elif land_type_row in CUA_HANG_MAT_BANG_LIST:
            land_type_col[row] = "Cửa Hàng - Mặt Bằng"
        
        elif land_type_row in DAT_CONG_NONG_NGHIEP_LIST:
            land_type_col[row] = "Đất công – nông nghiệp"

        elif land_type_row in DAT_NEN_LIST:
            land_type_col[row] = "Đất Nền"

        elif land_type_row in KHACH_SAN_LIST:
            land_type_col[row] = "Khách Sạn"

        elif land_type_row in KHO_XUONG_LIST:
            land_type_col[row] = "Kho Xưởng"

        elif land_type_row in NHA_PHO_LIST:
            land_type_col[row] = "Nhà Phố"

        elif land_type_row in PHONG_TRO_LIST:
            land_type_col[row] = "Phòng Trọ"

        elif land_type_row in RESORT_LIST:
            land_type_col[row] = "Resort"

        elif land_type_row in VAN_PHONG_LIST:
            land_type_col[row] = "Văn Phòng"
            
        else:
            land_type_col[row] = "Khác"

        print(land_type_col[row])

    raw_df.LAND_TYPE = land_type_col

    return raw_df

def clean_square(raw_df, usr, pwd, host, db, tb):

    raw_df_cols = raw_df.columns
    length_df = raw_df.shape[0]
    clean_df = pd.DataFrame(columns=raw_df_cols)
    comma_site_list = ['batdongsan', 'muabannhadat']
    dot_site_list_with_comma = ['TINBATDONGSAN']

    for row in range(length_df):
        try:
            square_row = raw_df.loc[row, 'SQUARE']
            site_row = raw_df.loc[row, 'SITE']
        except KeyError:
            continue

        if square_row is None or square_row == '--':
            mess = "Error code 003: value in column SQUARE is NULL"
            set_error_flag_to_db_and_log_message(raw_df.loc[row,:], mess, usr, pwd, host, db, tb)
            continue
        else:
            if site_row in comma_site_list:
                square_row = square_row.replace(',', '.')
            elif site_row in dot_site_list_with_comma:
                square_row = square_row.replace(',', '')
            
            square_row = square_row.replace('m²', '')
            square_row = square_row.replace('m2', '')
            square_row = square_row.replace('m��', '')
            
        raw_df.loc[row, 'SQUARE'] = float(square_row)
        clean_df = clean_df.append(raw_df.loc[row, :])        
        
    return clean_df

def get_unit_square(raw_df):
    raw_df['UNIT_SQUARE'] = 'M2'
    return raw_df

def clean_land_types(raw_df, land_type_dict, usr, pwd, host, db, tb):
    raw_df_cols = raw_df.columns
    length_df = raw_df.shape[0]
    clean_df = pd.DataFrame(columns=raw_df_cols)

    for row in range(length_df):
        try:
            land_type_row = raw_df.loc[row, 'LAND_TYPE']
        except KeyError:
            continue
        value_for_key = land_type_dict.get(land_type_row)
        
        if value_for_key is None:
            mess = "Error code 001: IN COLUMN LAND_TYPE ~ VALUE: (" + str(land_type_row) + ") - VALUE IN CONFIG MAXTRIX: (" + str(value_for_key) + ")"
            set_error_flag_to_db_and_log_message(raw_df.loc[row, :], mess, usr, pwd,host, db, tb)
            continue
        else:
            raw_df.loc[row, 'LAND_TYPE'] = value_for_key
            clean_df = clean_df.append(raw_df.loc[row, :])

    return clean_df

def get_v_unit_price(raw_df, usr, pwd, host, db, tb):
    cols_to_assess = ["SALE_TYPE", "SQUARE", "PRICE", "UNIT_PRICE"]
    raw_df["V_UNIT_PRICE"] = 0.0

    for inx, row in raw_df.iterrows():
        null_in_cols_to_assess = row[cols_to_assess].isnull().any()

        if not null_in_cols_to_assess:
            
            unit_price = row["UNIT_PRICE"]
            price = float(row["PRICE"])
            if "m2" in unit_price:
                if "trăm" in unit_price:
                    raw_df.at[inx, "V_UNIT_PRICE"] = price / 10
                elif "nghìn" in unit_price:
                     raw_df.at[inx, "V_UNIT_PRICE"] = price / 1000
                elif "tỷ" in unit_price:
                    raw_df.at[inx, "V_UNIT_PRICE"] = price * 1000
                else:
                    raw_df.at[inx, "V_UNIT_PRICE"] = price
            else:
                square = float(row["SQUARE"])
                if square == 0:
                    mess = "Error code 002: VALUE IN SQUARE COLUMN IS 0"
                    set_error_flag_to_db_and_log_message(row, mess, usr, pwd, host, db, tb)
                    continue
                else:
                    if unit_price == "tỷ":
                        raw_df.at[inx, "V_UNIT_PRICE"] = price * 1000 / square
                    elif unit_price == "triệu":
                        raw_df.at[inx, "V_UNIT_PRICE"] = price / square
                    
    return raw_df

def get_amount(raw_df):
    raw_df['AMOUNT'] = 0.0

    for inx, row in raw_df.iterrows():
        raw_df.at[inx, 'AMOUNT'] = row['V_UNIT_PRICE'] * row['SQUARE']
    
    return raw_df

                

                
                

                    

                
                        
                        
                    
            
            


