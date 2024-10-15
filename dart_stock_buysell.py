import requests
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import numpy as np
import re
import time




class dart_stock_buysell:

    def stock_buysell(dartkey,dart,biz_day):
        
        lists = dart.list(start=biz_day, end=biz_day)
        list_df = lists[lists['report_nm']== '임원ㆍ주요주주특정증권등소유상황보고서']
        list_df = list_df[~list_df['corp_name'].str.contains('리츠')]
        print(f'{biz_day} {len(list_df)}개 조회' )
        
        ddf = pd.DataFrame()
        
        try:
            for rcept_no,corp_name,flr_nm,rcept_dt in zip(list_df['rcept_no'],list_df['corp_name'],list_df['flr_nm'],list_df['rcept_dt']):  
                res = dart.sub_docs(rcept_no)['url'][3]
                call = requests.get(res)
                soup = BeautifulSoup(call.text, "html.parser")  
                before = soup.select('body > table')
                if '사유' in parser.make2d(before[-1])[0][0]:
                    table = parser.make2d(before[-1])
                elif '사유' in parser.make2d(before[-2])[0][0]:
                    table = parser.make2d(before[-2])
                data = pd.DataFrame(table,columns=table[1])
                data = data.drop(index=[0,1])
                data = data.drop(data.index[-1:])
                data.insert(0,'종목명',corp_name)
                data.insert(0,'기준일',rcept_dt)
                data.insert(9,'보고자',flr_nm)
                # data = data[data['보고사유'].str.contains('장내매수|장내매도')]
                data['변동일*'] = data['변동일*'].str.strip().str.replace('년 ','.').str.replace('월 ','.').str.replace('일','')
                data.columns = ['기준일','종목명','보고사유','변동일','증권종류','변동전','증감','변동후','단가','보고자','비고']
                data.drop(columns='비고',inplace=True)
                data = data.replace({'-':None})
                data = data[~data['종목명'].str.contains('리츠')]
                data['단가'] = data['단가'].apply(lambda x: re.sub('[^0-9]', '', str(x)) if x is not None else '').replace('', '0').astype(int)
                data['변동전'] = data['변동전'].str.replace(',','').fillna(0).astype(int)
                data['증감'] = data['증감'].str.replace(',','').fillna(0).astype(int)
                data['변동후'] = data['변동후'].str.replace(',','').fillna(0).astype(int)
                data['금액'] = round((data['증감'] * data['단가'])/100000000,2)
                data = data.replace({np.nan:None})
                ddf = pd.concat([ddf,data])
                time.sleep(3)
        except Exception as e:
            print(f'{corp_name} 실패')
        print(f'[{biz_day}] [dart_stock_buysell] {len(ddf)}개 로딩 성공')
        return ddf

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
            insert into dart_stock_buysell (기준일,종목명,보고사유,변동일,증권종류,변동전,증감,변동후,단가,보고자,금액)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            보고사유 = new.보고사유,증권종류=new.증권종류,변동전=new.변동전,증감=new.증감,변동후=new.변동후,단가=new.단가,금액=new.금액;
        """
  
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'[{biz_day}][dart_stock_buysell] {len(df)}개 DB INSERT 성공')
        con.close()
        
        
# if __name__ == '__main__':
#     dd = dart_stock_buysell.stock_buysell()
#     print(dd)
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
