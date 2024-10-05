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


biz_day = date_biz_day()

# 코스피
gen_otp_url='http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_stk = {
    'mktId': 'STK',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    
}
headers = {
    'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
           }

otp_stk = requests.post(gen_otp_url,gen_otp_stk,headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_stk = requests.post(down_url, {'code':otp_stk}, headers=headers)

sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')

# 코스닥
gen_otp_url='http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_ksq = {
    'mktId': 'KSQ',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}
headers = {
    'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
           }

otp_ksq = requests.post(gen_otp_url,gen_otp_ksq,headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_ksq = requests.post(down_url, {'code':otp_ksq}, headers=headers)

sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR')

krx_sector = pd.concat([sector_stk,sector_ksq]).reset_index(drop=True)
krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day


# per/pbr
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

krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day


kor_ticker = pd.merge(krx_sector,
                      krx_ind,
                      on=krx_sector.columns.intersection(krx_ind.columns).tolist(),
                      how='outer')

diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))
kor_ticker['종목구분']=  np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'),'스팩',
                        np.where(kor_ticker['종목코드'].str[-1:] != '0','우선주',
                        np.where(kor_ticker['종목명'].str.endswith('리츠'),'리츠',
                        np.where(kor_ticker['종목명'].isin(diff),'기타','보통주'
                                ))))

kor_ticker = kor_ticker.reset_index(drop=True)
kor_ticker.columns = kor_ticker.columns.str.replace(' ','')
kor_ticker = kor_ticker[['종목코드','종목명','시장구분','종가','시가총액','기준일','EPS','선행EPS','BPS','PER','PBR','주당배당금','종목구분']]
kor_ticker = kor_ticker.replace({np.nan:None})
kor_ticker.to_clipboard()
con = pymysql.connect(user='root',
                      passwd='dkvkxm8093!',
                      host = '127.0.0.1',
                      db='stock',
                      charset='utf8'                      
                      )
mycursor = con.cursor()
query = f"""
    insert into kor_ticker (종목코드,종목명,시장구분,종가,시가총액,기준일,EPS,선행EPS,BPS,PER,PBR,주당배당금,종목구분)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    종목명 = new.종목명,시장구분=new.시장구분,종가=new.종가,시가총액=new.시가총액,EPS=new.EPS,선행EPS=new.선행EPS,
    BPS=new.BPS,PER=new.PER,PBR=new.PBR,주당배당금=new.주당배당금,종목구분=new.종목구분;
"""
args = kor_ticker.values.tolist()
mycursor.executemany(query,args)
con.commit()

con.close()

