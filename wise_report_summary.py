import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
from datetime import datetime
import numpy as np
import pymysql

today = datetime.today().strftime('%Y%m%d')

def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.content)

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day


# 코스피
gen_otp_url='https://comp.wisereport.co.kr/wiseReport/summary/ReportSummary.aspx?cn='
gen_otp_stk = {
'ddlDate': '20241008',
}
headers = {
    'Referer':'https://comp.wisereport.co.kr/wiseReport/summary/ReportSummary.aspx?cn=',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
           }

otp_stk = requests.post(gen_otp_url,gen_otp_stk,headers=headers).text
print(otp_stk)

# down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
# down_sector_stk = requests.post(down_url, {'code':otp_stk}, headers=headers)

# sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
