import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import xlwings as xw
from urllib.request import urlopen 
from urllib.parse import quote_plus #한글을 아스키코드로 변환
from tqdm import tqdm
import time
# http://comp.fnguide.com/ 요약리포트 가져와서 엑셀에 저장
today = (datetime.today()).strftime('%Y%m%d')

def fnguide_summary_report(start_day,end_day):
    header = {'User-agent': 'Mozila/2.0'}
    url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Report_Summary_Data.asp?fr_dt={start_day}&to_dt={end_day}&stext=&check=all&sortOrd=5&sortAD=A&_=1682769602126'
    html = rq.get(url,headers = header)
    soup=BeautifulSoup(html.text,'html.parser')
    trdata = soup.find_all('tr') # 테이블에 해당하는 html 만 불러옴
    lists = []
    for i in range(0,len(trdata)):
        a = trdata[i].text.split('\n') #가로로 되어 있어 세로로 변경
        data = [ i for i in a if i] # 빈칸 제거 
        lists.append(data)
        time.sleep(1)
    df = pd.DataFrame(lists)
    df['기업명'] = df[1].str.split(' -').str[0]
    df[1] = df[1].str.split(' -').str[1]
    contend_merge = [1,2,3]
    df['내용'] = df[contend_merge].apply(lambda x: '\n'.join(x.values),axis=1) # 내용이 3개 컬럼으로 나눠져있어 하나의 컬럼으로 합치기
    df.columns = ['날짜','1','2','3','투자의견','목표주가','전일종가','제공자','기업명','리포트요약']
    df = df[['기업명','리포트요약','투자의견','목표주가','전일종가','제공자','날짜']]
    df['기업명'] = df['기업명'].str.split(' ').str[0] # 기업명과기업코드가 같이 있어 나눠줌
    # df['월'] = df['날짜'].str.split('/').str[1]
    df=df.set_index('기업명')
    return df        

df = fnguide_summary_report(today,today)
print(df)