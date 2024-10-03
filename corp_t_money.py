import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
from datetime import datetime
import numpy as np
import pymysql


def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.content)

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day


biz_day = date_biz_day()

def corp_trading(code, corp,biz_day):
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp = {
        'locale': 'ko_KR',
        'inqTpCd': '2',
        'trdVolVal': '2',
        'askBid': '3',
        'tboxisuCd_finder_stkisu0_1': f'{code}/{corp}',
        'isuCd': 'KR7005930003',
        'isuCd2': 'KR7005930003',
        'codeNmisuCd_finder_stkisu0_1': f'{corp}',
        'param1isuCd_finder_stkisu0_1': 'ALL',
        'strtDd': biz_day,
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
    corp_trading_df.set_index('일자',inplace=True)
    corp_trading_df = corp_trading_df.apply(lambda x : round(x/100000000,1))
    corp_trading_df.insert(0,'기업',corp)
    return corp_trading_df

df = corp_trading('005930','삼성전자',biz_day)
print(df)