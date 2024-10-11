import requests
from bs4 import BeautifulSoup
import pymysql
import warnings
from key.db_info import connectDB
import pandas as pd
import numpy as np
import re
warnings.filterwarnings("ignore")

def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.content)

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day


def down_whynot_report(db_info):
    con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
    mycursor = con.cursor()
    sql = """
        select * from whynot_report
        """
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    return result

if __name__ == '__main__':
    db_info = connectDB.db_conn()
    date = date_biz_day()
    date = pd.to_datetime(date).strftime('%Y-%m-%d')  # '20240904'
    data = down_whynot_report(db_info)
    df = pd.DataFrame(data,columns=['id','date','company_name','analyst_name','target_price','judge','title','description','analyst_rank','stock_code_id','analyst_id'])
    # df.to_excel('whynot_report_20240101_20241010.xlsx',index=False)
    
    # 날짜별로 정렬 후 groupby로 각 회사/애널리스트별 이전 목표가 찾기
    df_sorted = df.sort_values(by=['company_name', 'analyst_name', 'date'])
    
    df_sorted['prev_target_price'] = df_sorted.groupby(['company_name', 'analyst_name'])['target_price'].shift(1)
    df_sorted['change'] = df_sorted.apply(lambda row: '상향' if row['target_price'] > row['prev_target_price'] else '하향', axis=1)
    df_sorted['date'] = pd.to_datetime(df_sorted['date'])
    df_sorted['date'] = df_sorted['date'].dt.strftime('%Y-%m-%d')
    df_final = df_sorted[df_sorted['date'] == date][['date','company_name','analyst_name','change','prev_target_price','target_price','judge','title','description']]
    df_final.columns = ['날짜','기업명','애널리스트','구분','이전TP','현재TP','판단','제목','내용']
    df_final = df_final.sort_values(by=['구분','기업명','애널리스트'],ascending=True)
    
    df_final['상승률'] = round(((df_final['현재TP']-df_final['이전TP'])/df_final['이전TP']*100),1)
    df_final['상승률'] = np.where(df_final['이전TP'] == 0, '신규', df_final['상승률'])
    df_final = df_final[['날짜', '기업명', '애널리스트', '구분', '이전TP', '현재TP', '상승률', '판단', '제목', '내용']]
    df_final.to_excel(f'whynot_report_final_{date}.xlsx',index=False)
    
    print(df_final)
    