import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.request import urlopen 
from urllib.parse import quote_plus #한글을 아스키코드로 변환
import re
import time
import pymysql
import warnings
from key.db_info import connectDB
import json
import numpy as np
warnings.filterwarnings("ignore")


# http://comp.fnguide.com/ 요약리포트 가져와서 엑셀에 저장
today = (datetime.today()).strftime('%Y%m%d')
class whynot_report:
    # def date_biz_day():
    #     url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    #     res = rq.get(url)
    #     soup = BeautifulSoup(res.content)

    #     parse_day = soup.select_one('#time').text
        
    #     biz_day = re.findall('[0-9]+',parse_day)
    #     biz_day = ''.join(biz_day)
    #     return biz_day

    # def fnguide_summary_report(fr_dt,to_dt):
    #     header = {'User-agent': 'Mozila/2.0'}
    #     url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Report_Summary_Data.asp?fr_dt={fr_dt}&to_dt={to_dt}&stext=&check=all&sortOrd=5&sortAD=A&_=1682769602126'
    #     html = rq.get(url,headers = header)
    #     soup=BeautifulSoup(html.text,'html.parser')
    #     trdata = soup.find_all('tr') # 테이블에 해당하는 html 만 불러옴
    #     lists = []
    #     for i in range(0, len(trdata)):
    #         datas = trdata[i].text.split('\n')
    #         data = [ i for i in datas if i] # 빈칸 제거 
    #         if len(data) == 8:
    #             lists.append(data)
    #         elif len(data) == 7:
    #             data.insert(3, '')  # 3번째 자리에 빈칸 추가 (인덱스 2)
    #             lists.append(data)
    #     df = pd.DataFrame(lists)
    #     df['기업명'] = df[1].str.split(' -').str[0]
    #     df[1] = df[1].str.split(' -').str[1]
    #     contend_merge = [1,2,3]
    #     df['내용'] = df[contend_merge].apply(lambda x: '\n'.join(x.values),axis=1) # 내용이 3개 컬럼으로 나눠져있어 하나의 컬럼으로 합치기
    #     df.columns = ['날짜','1','2','3','투자의견','목표주가','전일종가','제공자','기업명','리포트요약']
    #     df = df[['기업명','리포트요약','투자의견','목표주가','전일종가','제공자','날짜']]
    #     df['기업명'] = df['기업명'].str.split(' ').str[0] # 기업명과기업코드가 같이 있어 나눠줌
    #     # df['월'] = df['날짜'].str.split('/').str[1]
    #     df['목표주가'] = df['목표주가'].str.replace(',', '')  # 쉼표 제거
    #     df['목표주가'] = df['목표주가'].str.replace('\xa0', ' ')  # \xa0을 공백으로 대체
    #     df['목표주가'] = df['목표주가'].str.strip()  # 양쪽 공백 제거
    #     df['목표주가'] = df['목표주가'].replace('', '0')  # 빈 문자열을 0으로 대체
    #     df['목표주가'] = df['목표주가'].astype(int)
        
    #     df['전일종가'] = df['전일종가'].str.replace(',', '')  # 쉼표 제거
    #     df['전일종가'] = df['전일종가'].str.replace('\xa0', ' ')  # \xa0을 공백으로 대체
    #     df['전일종가'] = df['전일종가'].str.strip()  # 양쪽 공백 제거
    #     df['전일종가'] = df['전일종가'].replace('', '0')  # 빈 문자열을 0으로 대체
    #     df['전일종가'] = df['전일종가'].astype(int)
    #     df['괴리율(%)'] = round(((df['목표주가'] - df['전일종가'])/df['전일종가'])*100,1)
    #     df['글자수'] = df['리포트요약'].apply(len)
    #     time.sleep(2)
    #     return df


    def whynot_report(biz_day):
        url = f'https://www.whynotsellreport.com/api/reports/from/{biz_day}/to/{biz_day}'
        html = rq.get(url)
        # 필요한 필드만 추출하여 리스트로 만들기
        data = html.json()
        
        extracted_data = []
        for report in data:
            extracted_data.append({
                'id': report['id'],
                'date': report['date'],
                'company_name': report['company_name'],
                'analyst_name': report['analyst_name'],
                'price': report['price'],
                'judge': report['judge'],
                'title': report['title'],
                'description': report['description'],
                'analyst_rank': report['analyst_rank'],
                'stock_code_id': report['stock_code_id'],
                'analyst_id': report['analyst_id'],
            })

    # 데이터프레임 생성
        df = pd.DataFrame(extracted_data)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y%m%d')
        df = df.rename(columns={'price':'target_price'})
        print(f'{biz_day} [whynot_report] {len(df)}개 로딩 성공')
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
            insert into whynot_report (id,date,company_name,analyst_name,target_price,judge,title,description,analyst_rank,stock_code_id,analyst_id)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            date=new.date,company_name=new.company_name,analyst_name=new.analyst_name,target_price=new.target_price,
            judge=new.judge,title=new.title,description=new.description,analyst_rank=new.analyst_rank,stock_code_id=new.stock_code_id,analyst_id=new.analyst_id;
        """
        df = df.replace({np.nan: None})
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'{biz_day} [whynot_report] {len(df)}개 DB INSERT 성공')
        con.close()








if __name__ == '__main__':
    db_info = connectDB.db_conn()
    to_dt = date_biz_day()
    to_dt = pd.to_datetime(to_dt).strftime('%Y-%m-%d')  # '20240904'
    fr_dt = pd.to_datetime('20241001').strftime('%Y-%m-%d')  # '20240904'
    
    print(fr_dt,to_dt)
    df = whynot_report(fr_dt,to_dt)

    # df = fnguide_summary_report(fr_dt,to_dt)
    db_insert(db_info,df)
    # df.to_excel(f'whynot{fr_dt}_{to_dt}.xlsx')