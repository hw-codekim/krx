import requests
import json
import pymysql
import pandas as pd
import time
from tqdm import tqdm
from key.db_info import connectDB


def dart_key():
    with open("./key/dart_api_key.json",'r',encoding='utf-8') as f:
        key = json.load(f)   

    dart_key = key['dart_api_key']
    return dart_key


def loadDB_dart_code(db_info):
    con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
    mycursor = con.cursor()
    sql = f"""
        SELECT corp_code,corp_name,stock_code FROM dart_code
        """
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    return result



def loadDart_employee(dart_key,corp_code,bsns_year,reprt_code):
    url = 'https://opendart.fss.or.kr/api/empSttus.json'
    params = {
        'crtfc_key' : dart_key,
        'corp_code' : corp_code, # 
        'bsns_year' : bsns_year,
        'reprt_code' : reprt_code
    }
    res = requests.get(url,params=params)
    data = res.json()
    df = pd.DataFrame(data['list'])
    df = df[['corp_name','fo_bbm','sexdstn','rgllbr_co','cnttk_co']]
    df.columns = ['종목명','사업부문','성별','정규직수','계약직수']
    df = df.apply(lambda x : x.replace('-',0))

    df['정규직수'] = df['정규직수'].astype(str).str.replace(',', '').astype(int)
    df['계약직수'] = df['계약직수'].astype(str).str.replace(',', '').astype(int)
    # df = df.apply(pd.to_numeric,errors = 'ignore')

    if reprt_code == '11013':
        df.insert(1,'q','1Q')
    elif reprt_code == '11012':
        df.insert(1,'q','2Q')
    elif reprt_code == '11014':
        df.insert(1,'q','3Q')
    elif reprt_code == '11011':
        df.insert(1,'q','4Q') 

    df.insert(1,'년도',bsns_year)
    df['분기'] = df['년도'] +'.'+ df['q']

    df = df[['종목명','분기','사업부문','성별','정규직수','계약직수']]
    df = df[~df['사업부문'].str.contains('합계')]
    df['정규직수'] = df['정규직수'].fillna(0)
    df['계약직수'] = df['계약직수'].fillna(0)
    df['합계'] = df['정규직수'] + df['계약직수']
    # 정규직수, 계약직수의 총합 계산
    result = df.groupby(['종목명', '분기'], as_index=False).agg({
    '정규직수': 'sum',
    '계약직수': 'sum',
    '합계': 'sum'
    })
    time.sleep(1)
    return result
    
    

def insert_db_dart_employee(df,db_info,name):
        con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
        mycursor = con.cursor()
        query = f"""
            insert into dart_employee (종목명,분기,정규직수,계약직수,합계)
            values (%s,%s,%s,%s,%s) as new
            on duplicate key update
            정규직수 = new.정규직수,계약직수=new.계약직수,합계=new.합계;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'[{name}][dart_employee] DB INSERT 성공')
        con.close()
        return  
    
if __name__ == '__main__':
    db_info = connectDB.db_conn()
    dartkey = dart_key()
    
    code_ = loadDB_dart_code(db_info)
    
    bsns_years = ['2016','2017','2018','2019','2020','2021','2022','2023','2024']
    
    reprt_codes = ['11013','11012','11014','11011']
    
    code_lists = pd.DataFrame(code_,columns=['corp_code','name','stock_code'])
    # print(code_lists)
    # # corp_code = '01170962' #grt
    # corp_code = '00126380' #kcc글라스
    # aa = loadDart_employee(dartkey,corp_code,'2020','11013')
    # print(aa)2018_11012
    
    result_df = pd.DataFrame()
    for corp_code,name in zip(code_lists['corp_code'].iloc[10:],code_lists['name'].iloc[10:]):
        print(name)
        result_df = pd.DataFrame()
        for bsns_year in bsns_years:
            for reprt_code in reprt_codes:
                try:
                    result = loadDart_employee(dartkey,corp_code,bsns_year,reprt_code)
                    result_df = pd.concat([result_df,result])
                except Exception as e:
                    print(f'[{bsns_year}_{reprt_code}]',e)
        insert_db_dart_employee(result_df,db_info,name)
    # 하나기술까지 함. 하나기술 52번, 53번부터 할 차례 2024_10_13일
    