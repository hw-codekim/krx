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

class krx_sise:

    def daily_kospi(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '02',
            'trdDd': biz_day,
            'share': '2',
            'money': '4',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        daily_kospi_value = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        
        daily_kospi_value = daily_kospi_value.replace({np.nan:None})
        daily_kospi_value['지수명'] = daily_kospi_value['지수명'].str.replace(' ','')
        daily_kospi_value = daily_kospi_value[~daily_kospi_value['지수명'].str.contains('외국주')]
        daily_kospi_value = daily_kospi_value.apply(pd.to_numeric,errors = 'ignore')
        daily_kospi_value.insert(0,'기준일',biz_day)
        print(f'[{biz_day}] [krx_daily_kospi] {len(daily_kospi_value)}개 로딩 성공')
        return daily_kospi_value
    
    def daily_kosdaq(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '03',
            'trdDd': biz_day,
            'share': '2',
            'money': '4',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        daily_kosdaq_value = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        daily_kosdaq_value = daily_kosdaq_value.replace({np.nan:None})
        daily_kosdaq_value['지수명'] = daily_kosdaq_value['지수명'].str.replace(' ','')
        daily_kosdaq_value = daily_kosdaq_value[~daily_kosdaq_value['지수명'].str.contains('외국주')]
        daily_kosdaq_value = daily_kosdaq_value.apply(pd.to_numeric,errors = 'ignore')
        daily_kosdaq_value.insert(0,'기준일',biz_day)
        print(f'[{biz_day}] [krx_daily_kosdaq] {len(daily_kosdaq_value)}개 로딩 성공')
        return daily_kosdaq_value
    
    def merge_sise(biz_day):
        
        kospi = krx_sise.daily_kospi(biz_day)
        kosdaq = krx_sise.daily_kosdaq(biz_day)
        df = pd.concat([kospi,kosdaq])
        return df

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
            insert into krx_sise (기준일,지수명,종가,대비,등락률,시가,고가,저가,거래량,거래대금,상장시가총액)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            종가 = new.종가,대비=new.대비,등락률=new.등락률,시가=new.시가,고가=new.고가,저가=new.저가,거래량=new.거래량,
            거래대금=new.거래대금,상장시가총액=new.상장시가총액;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'[{biz_day}] [krx_sise] {len(df)}개 DB INSERT 성공')
        con.close()
        time.sleep(2)
        
if __name__ == '__main__':
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    
    biz_day_format = pd.to_datetime(biz_day)
    ago_format = biz_day_format - timedelta(days=365)
    date_range = pd.date_range(ago_format, biz_day_format)

    for day in date_range:
        day_str = day.strftime('%Y%m%d')  # YYYYMMDD 형식으로 변환

        try:
            df = krx_sise.merge_sise(day_str)
            krx_sise.insertDB(day_str,df,db_info)
        except Exception as e:
            print(f'{day}',e)
