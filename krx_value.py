import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
import pymysql
from key.db_info import connectDB
import time
from biz_day import date_biz_day

class krx_value:

    def daily_value(biz_day):
        gen_otp_url='http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp_data = {
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': biz_day,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }
        headers = {
            'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp = requests.post(gen_otp_url,gen_otp_data,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        krx_ind = requests.post(down_url, {'code':otp}, headers=headers)

        data = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
        data['종목명'] = data['종목명'].str.strip()
        data.insert(0,'기준일',biz_day)
        data.columns = data.columns.str.replace(' ','')
        data = data.replace({np.nan:None})
        print(f'{biz_day} [krx_value] {len(data)}개 로딩 성공')
        return data

    def insertDB(biz_day,df,db_info):
        con = pymysql.connect(
                    user=db_info[0],
                    password=db_info[1],
                    host = db_info[2],
                    port = int(db_info[3]),
                    database=db_info[4],                     
                    )
        mycursor = con.cursor()
        query = f"""
            insert into krx_value (기준일,종목코드,종목명,종가,대비,등락률,EPS,PER,선행EPS,선행PER,BPS,PBR,주당배당금,배당수익률)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            종목명 = new.종목명,종가=new.종가,대비=new.대비,등락률=new.등락률,EPS=new.EPS,PER=new.PER,선행EPS=new.선행EPS,
            선행PER=new.선행PER,BPS=new.BPS,PBR=new.PBR,주당배당금=new.주당배당금,배당수익률=new.배당수익률;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'{biz_day} [krx_value] {len(df)}개 DB INSERT 성공')
        con.close()
    
if __name__ == '__main__':
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    df = krx_value.daily_value(biz_day)
    krx_value.insertDB(biz_day,df,db_info)
