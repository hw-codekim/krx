import requests
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time

class dartList:

    def loadDart_list(dartkey,bgn_de,end_de,page_no,page_count,search):

        url = '	https://opendart.fss.or.kr/api/list.json'
        params = {
            'crtfc_key' : dartkey,
            'bgn_de' : bgn_de, # 
            'end_de' : end_de, # 
            'page_no' : page_no, # 
            'page_count' : page_count, # 
        }
        
        res = requests.get(url,params=params)
        data = res.json()
        df = pd.DataFrame(data['list'])
        df = df[df['report_nm'].isin(search)]
        print(f'{len(df)}개 항목')
        time.sleep(2)
        return df

# def document(dart,rcept_no,corp_name,flr_nm,rcept_dt):
#     res = dart.sub_docs(rcept_no)['url'][3]
#     call = requests.get(res)
#     soup = BeautifulSoup(call.text, "html.parser")
#     before = soup.select('body > table')
#     if '사유' in parser.make2d(before[-1])[0][0]:
#         table = parser.make2d(before[-1])
#     elif '사유' in parser.make2d(before[-2])[0][0]:
#         table = parser.make2d(before[-2])
#     data = pd.DataFrame(table,columns=table[1])
#     data = data.drop(index=[0,1])
#     data = data.drop(data.index[-1:])
#     data.insert(0,'종목명',corp_name)
#     data.insert(0,'기준일',rcept_dt)
#     data.insert(9,'보고자',flr_nm)
#     data = data[data['보고사유'].str.contains('장내매수|장내매도')]
#     data['변동일*'] = data['변동일*'].str.strip().str.replace('년 ','.').str.replace('월 ','.').str.replace('일','')
#     # data['취득/처분단가(원)**'] = data['취득/처분단가(원)**'].str.strip().str.replace('원','').str.replace('(','').str.replace(')','').str.replace('취득','').str.replace('/','').str.replace(',','').str.replace('해당없음','')
#     data.columns = ['기준일','종목명','보고사유','변동일','증권종류','변동전','증감','변동후','단가','보고자','비고']
#     data.drop(columns='비고',inplace=True)
#     data = data.apply(lambda x : x.replace('-','0'))
#     data['단가'] = data['단가'].str.replace(',','').str.replace('원','').str.replace('(','').str.replace(')','').str.replace('취득','').str.replace('/','').str.replace('해당없음','').astype(int)
#     data['변동전'] = data['변동전'].str.replace(',','').astype(int)
#     data['증감'] = data['증감'].str.replace(',','').astype(int)
#     data['변동후'] = data['변동후'].str.replace(',','').astype(int)
#     data['금액'] = round((data['증감'] * data['단가'])/100000000,2)
#     return data

# if __name__ == '__main__':
#     date = date_biz_day()
#     dartkey = dart_key()
#     dart = OpenDartReader(dartkey)
#     bgn_de = date
#     end_de = date
#     page_count = 100
#     search = ['임원ㆍ주요주주특정증권등소유상황보고서']
    
#     df = pd.DataFrame()
#     for page_no in range(1,10):
#         lists = loadDart_list(dartkey,bgn_de,end_de,page_no,page_count,search)
#         df = pd.concat([df,lists])

    
#     ttl_df = pd.DataFrame()

#     for rcept_no,corp_name,flr_nm,rcept_dt  in zip(df['rcept_no'],df['corp_name'],df['flr_nm'],df['rcept_dt']):
#         dd= document(dart,rcept_no,corp_name,flr_nm,rcept_dt)
#         ttl_df = pd.concat([ttl_df,dd])

#     print(ttl_df.info())
    