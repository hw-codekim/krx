import json
import requests
import time
import pandas as pd
from key.db_info import connectDB
import pymysql
from tqdm import tqdm



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

def loadDart_stock_buysell(dart_key,corp_code):
    url = 'https://opendart.fss.or.kr/api/elestock.json'
    params = {
        'crtfc_key' : dart_key,
        'corp_code' : corp_code, # 
    }
    res = requests.get(url,params=params)
    data = res.json()
    df = pd.DataFrame(data['list'])
    df = df[['rcept_dt','corp_name','repror','isu_exctv_rgist_at','isu_exctv_ofcps','isu_main_shrholdr','sp_stock_lmp_cnt','sp_stock_lmp_irds_cnt','sp_stock_lmp_rate','sp_stock_lmp_irds_rate']]
    df.columns = ['기준일','종목명','보고자','임원','직위','주요주주','주식수','증감수','비율','증감비율']
    df['주식수'] = df['주식수'].str.replace(',','')
    df['증감수'] = df['증감수'].str.replace(',','')
    df['비율'] = df['비율'].str.replace(',','')
    df['증감비율'] = df['증감비율'].str.replace(',','')
    df = df.apply(pd.to_numeric,errors = 'ignore')
    time.sleep(1)
    return df
        
def db_insert(df,db_info):
    con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
    mycursor = con.cursor()
    query = f"""
        insert into dart_stock_buysell (기준일,종목명,보고자,임원,직위,주요주주,주식수,증감수,비율,증감비율)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
        on duplicate key update
        임원 = new.임원,직위=new.직위,주요주주=new.주요주주,주식수=new.주식수,증감수=new.증감수,비율=new.비율,증감비율=new.증감비율;
    """
    args = df.values.tolist()
    mycursor.executemany(query,args)
    con.commit()
    print(f'[dart_stock_buysell] DB INSERT 성공')
    con.close()   
        
        
        
        
if __name__ == '__main__':
    db_info = connectDB.db_conn()
    dartkey = dart_key()
    
    code_ = loadDB_dart_code(db_info)
    code_lists = pd.DataFrame(code_,columns=['corp_code','name','stock_code'])
    code_lists['corp_code'] = code_lists['corp_code'].astype(str)

    for code,name in tqdm(zip(code_lists['corp_code'],code_lists['name'])):
        try:
            df = loadDart_stock_buysell(dartkey,code)
            db_insert(df,db_info)
            print(df)
        except Exception as e:
            print(name, e)
