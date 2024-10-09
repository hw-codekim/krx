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



def loadDart_employee(dart_key,corp_code:str,bsns_year,reprt_code):
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
    df['정규직수'] = df['정규직수'].str.replace(',','')
    df['계약직수'] = df['계약직수'].str.replace(',','')
    df = df.apply(pd.to_numeric,errors = 'ignore')
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
    time.sleep(1)
    return df
    
    

def insert_db_dart_employee(db_info,df):
        con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
        mycursor = con.cursor()
        query = f"""
            insert into dart_employee (종목명,분기,정규직수,계약직수)
            values (%s,%s,%s,%s) as new
            on duplicate key update
            정규직수 = new.정규직수,계약직수=new.계약직수;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        return  
    
if __name__ == '__main__':
    db_info = connectDB.db_conn()
    dartkey = dart_key()
    
    code_ = loadDB_dart_code(db_info)
    
    bsns_years = ['2020','2021','2022','2023','2024']
    reprt_codes = ['11013','11012','11014','11011']

    # print(code_lists)
    # for code in code_lists:
    #     print(code[2])
    #     print(type(code[0]))
    
    code_lists = pd.DataFrame(code_,columns=['corp_code','name','stock_code'])
    code_lists['corp_code'] = code_lists['corp_code'].astype(str)
    result_df = pd.DataFrame()
    for code in code_lists['corp_code']:
        print(code)
        # code = code.astype(str)
        for bsns_year in bsns_years:
            for reprt_code in reprt_codes:
                try:
                    result = loadDart_employee(dartkey,code,bsns_year,reprt_code)
                    result_df = pd.concat([result_df,result])
                except Exception as e:
                    print(f'[{bsns_year}_{reprt_code}]',e)
                
    # merge 한 데이터프레임을 다시 그룹으로 만들고 db 에 insert 하기
    try:
        quarterly_totals = result_df.groupby(['종목명','분기'])['정규직수','계약직수'].sum().reset_index()
        insert_db_dart_employee(db_info,quarterly_totals)
    except Exception as e:
        print(e)
    print(result_df)
    