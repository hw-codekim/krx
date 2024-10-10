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


def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.content)

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day

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

def corp_trading(spec_code,code, corp,biz_day,biz_day_start):
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
        'strtDd': biz_day_start, # 1년전
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
    time.sleep(2)
    return corp_trading_df

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
        insert into krx_trade_amount (기준일,종목코드,종목명,금융투자,보험,투신,사모,은행,기타금융,연기금등,기타법인,개인,외국인,기타외국인,전체)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
        on duplicate key update
        종목명 = new.종목명,금융투자=new.금융투자,보험=new.보험,투신=new.투신,사모=new.사모,은행=new.은행,기타금융=new.기타금융,
        연기금등=new.연기금등,기타법인=new.기타법인,개인=new.개인,외국인=new.외국인,기타외국인=new.기타외국인,전체=new.전체;
    """
    args = df.values.tolist()
    mycursor.executemany(query,args)
    con.commit()
    con.close()

if __name__ == '__main__':
    db_info = connectDB.db_conn()
    # 날짜 선택
    biz_day = date_biz_day()
    biz_day_end = datetime.strptime(biz_day,'%Y%m%d')
    one_year_ago = biz_day_end - timedelta(days=365)
    biz_day_start = one_year_ago.strftime("%Y%m%d")

    # 시총 5000억 이상된 기업만 소팅해서 표준코드 구하기

    amount = corp_amount(biz_day,db_info)
    
    amount_df = pd.DataFrame(amount,columns = ['기준일','종목코드','종목명'])
   
    
    corp_lists = corp_code(db_info)
    corp_lists_df = pd.DataFrame(corp_lists,columns = ['표준코드','종목코드','종목명'])
    final_lists = pd.merge(amount_df,corp_lists_df,how='left')
    final_lists = final_lists.dropna()
    print(final_lists)
    
    

    # 거래대금 가져오기
    rst_df = pd.DataFrame()
    for idx,row in tqdm(final_lists.iterrows()):
        code = row['종목코드']
        corp = row['종목명']
        spec_code = row['표준코드']

        df = corp_trading(spec_code,code,corp,biz_day,biz_day_start)
        rst_df = pd.concat([rst_df,df])

    db_insert(rst_df,db_info)
