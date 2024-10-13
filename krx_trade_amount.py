import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
import pymysql
import time
import random
from tqdm import tqdm
import warnings
import numpy as np
from datetime import datetime,timedelta
from key.db_info import connectDB
warnings.filterwarnings("ignore")

class krx_trade_amount:

    def corp_code(db_info):
        con = pymysql.connect(
                user=db_info[0],
                password=db_info[1],
                host = db_info[2],
                port = int(db_info[3]),
                database=db_info[4],                     
                )
        mycursor = con.cursor()
        sql = """
            SELECT 표준코드,종목코드,종목명 FROM krx_base_info
            """
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        return result


    def corp_amount(biz_day,db_info):
        con = pymysql.connect(
                user=db_info[0],
                password=db_info[1],
                host = db_info[2],
                port = int(db_info[3]),
                database=db_info[4],                     
                )
        mycursor = con.cursor()
        sql = f"""
            SELECT 기준일,종목코드,종목명 FROM krx_daily_price
            where 1=1
            AND 기준일 = '{biz_day}'
            AND 시가총액 >= '5000';
            """
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        return result

    # def corp_trading(spec_code,code, corp,biz_day,biz_day_start):
    def corp_trading(biz_day,db_info):
        amount = krx_trade_amount.corp_amount(biz_day,db_info)
        amount_df = pd.DataFrame(amount,columns = ['기준일','종목코드','종목명'])
    
        corp_lists = krx_trade_amount.corp_code(db_info)
        corp_lists_df = pd.DataFrame(corp_lists,columns = ['표준코드','종목코드','종목명'])
        final_lists = pd.merge(amount_df,corp_lists_df,how='left')
        final_lists = final_lists.dropna()
        
        rst_df = pd.DataFrame()
        for idx,row in tqdm(final_lists.iterrows()):
            try:
                code = row['종목코드']
                corp = row['종목명']
                spec_code = row['표준코드']
                
                
                gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
                gen_otp = {
                    'locale': 'ko_KR',
                    'inqTpCd': '2',
                    'trdVolVal': '2',
                    'askBid': '3',
                    'tboxisuCd_finder_stkisu0_1': f'{code}/{corp}',
                    'isuCd': spec_code,
                    'isuCd2': code,
                    'codeNmisuCd_finder_stkisu0_1': f'{corp}',
                    'param1isuCd_finder_stkisu0_1': 'ALL',
                    'strtDd': biz_day, # 1년전
                    'endDd': biz_day,
                    'detailView': '1',
                    'money': '1',
                    'csvxls_isNo': 'false',
                    'name': 'fileDown',
                    'url': 'dbms/MDC/STAT/standard/MDCSTAT02303'
                    }

                headers = {
                        'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                        }

                otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

                down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
                down_trading = requests.post(down_url, {'code':otp_stk}, headers=headers)
                corp_trading_df = pd.read_csv(BytesIO(down_trading.content), encoding='EUC-KR')
                corp_trading_df = corp_trading_df.apply(pd.to_numeric,errors = 'ignore')
                corp_trading_df.iloc[:, 1:] = corp_trading_df.iloc[:, 1:].apply(lambda x: round(x/100000000, 1))
                corp_trading_df.columns = corp_trading_df.columns.str.replace(' ','')
                corp_trading_df = corp_trading_df.rename(columns={'일자':'기준일'})
                corp_trading_df.insert(1,'종목명',corp)
                corp_trading_df.insert(1,'종목코드',code)
                corp_trading_df = corp_trading_df.replace({np.nan:0})
                rst_df = pd.concat([rst_df,corp_trading_df])
                print(f'[{biz_day}] [krx_trade_amount] {corp} 로딩 성공')
                time.sleep(2)
            except Exception as e:
                print(f'{corp} 로딩실패',e)
        return rst_df

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
            insert into krx_trade_amount (기준일,종목코드,종목명,금융투자,보험,투신,사모,은행,기타금융,연기금등,기타법인,개인,외국인,기타외국인,전체)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            종목명 = new.종목명,금융투자=new.금융투자,보험=new.보험,투신=new.투신,사모=new.사모,은행=new.은행,기타금융=new.기타금융,
            연기금등=new.연기금등,기타법인=new.기타법인,개인=new.개인,외국인=new.외국인,기타외국인=new.기타외국인,전체=new.전체;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'{biz_day} [krx_trade_amount] {len(df)}개 DB INSERT 성공')
        con.close()
