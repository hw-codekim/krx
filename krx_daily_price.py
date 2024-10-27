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


class krx_daily_price:

    def daily_price(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': biz_day,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        daily_updown = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        
        daily_updown['시가총액'] = round(daily_updown['시가총액']/100000000,0)
        daily_updown['거래대금'] = round(daily_updown['거래대금']/100000000,1)
        daily_updown = daily_updown.replace({np.nan:None})
        daily_updown['종목명'] = daily_updown['종목명'].str.strip()
        daily_updown.insert(0,'기준일',biz_day)
        print(f'[{biz_day}] [krx_daily_price] {len(daily_updown)}개 로딩 성공')
        time.sleep(2)
        return daily_updown

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
            insert into krx_daily_price (기준일,종목코드,종목명,시장구분,소속부,종가,대비,등락률,시가,고가,저가,거래량,거래대금,시가총액,상장주식수)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            종목명 = new.종목명,시장구분=new.시장구분,소속부=new.소속부,종가=new.종가,대비=new.대비,등락률=new.등락률,시가=new.시가,
            고가=new.고가,저가=new.저가,거래량=new.거래량,거래대금=new.거래대금,시가총액=new.시가총액,상장주식수=new.상장주식수;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'{biz_day} [krx_daily_price] {len(df)}개 DB INSERT 성공')
        con.close()
    
# if __name__ == '__main__':
#     biz_day = date_biz_day()
#     db_info = connectDB.db_conn()
    
#     biz_day_format = pd.to_datetime(biz_day)
#     ago_format = biz_day_format - timedelta(days=138)
#     date_range = pd.date_range(ago_format, biz_day_format)
#     for day in date_range:
#         day_str = day.strftime('%Y%m%d')  # YYYYMMDD 형식으로 변환
#         try:
#             df = krx_daily_price.daily_price(day_str)
#             krx_daily_price.insertDB(day_str,df,db_info)
#         except Exception as e:
#             print(f'{day}',e)