import requests
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
from datetime import datetime
import time
from biz_day import date_biz_day
from key.db_info import connectDB
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
class google_stocknews:
    def get_krx_daily_price(db_info,biz_day):
        con = pymysql.connect(
                user=db_info[0],
                password=db_info[1],
                host = db_info[2],
                port = int(db_info[3]),
                database=db_info[4],                     
                )
        mycursor = con.cursor()
        sql = f"""
            select * from krx_daily_price
            where 1=1
            AND 종목명 NOT LIKE ('%우')
            AND 종목명 NOT LIKE ('%우B')
            AND 종목명 NOT LIKE ('%우(전환)')
            AND 시가총액 >= 700
            and 기준일 = '{biz_day}'
            """
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        df = pd.DataFrame(result,columns=['기준일','종목코드','종목명','시장구분','소속부','종가','대비','등락률','시가','고가','저가','거래량','거래대금','시가총액','상장주식수'])
        df = df[['종목코드','종목명']]
        return df

    def get_search_google(db_info,biz_day):

        corp_lst = google_stocknews.get_krx_daily_price(db_info,biz_day)
        
        news_df = pd.DataFrame()
        today = datetime.now().strftime('%Y-%m-%d')
        for corp_name in tqdm(corp_lst['종목명']):
            try:
                dd = []
                url = f'https://news.google.com/rss/search?q={corp_name}&hl=ko&gl=KR&ceid=KR%3Ako'
                res = requests.get(url)
                soup = BeautifulSoup(res.text,'lxml')
                items = soup.find_all('item')
                for item in items:
                    pub_date = item.find('pubdate').get_text()
                    pub_date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d')
                    
                    # pub_date = '2024-10-25' #임시
                    # today = '2024-10-25' #임시
                    
                    if pub_date == today:
                        title = item.find('title').get_text()
                        link = item.find('description')
                        description_soup = BeautifulSoup(link.get_text(), 'html.parser')
                        a_tag = description_soup.find('a')
                        link = a_tag['href']
 
                        dd.append([pub_date,title,link])
                ddf = pd.DataFrame(dd, columns=['기준일','제목','링크'])
                ddf = ddf[~ddf['제목'].str.contains('조선비즈')]
                ddf = ddf.sort_values('기준일',ascending=False)
                ddf.insert(0,'종목명',corp_name)
                ddf.reset_index(drop=True,inplace=True)
                ddf = ddf.drop_duplicates('기준일')
                google_stocknews.insertDB(biz_day,ddf,db_info)
                news_df = pd.concat([news_df,ddf])
                # google_stocknews.insertDB(biz_day,news_df,db_info)
                # time.sleep(1)
                # print(news_df)
            except Exception as e:
                print(f'{corp_name} 검색실패',e)
        print(f'[{biz_day}] [google_news] {len(news_df)}개 로딩 성공')
        return news_df


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
            insert into google_stocknews (종목명,기준일,제목,링크)
            values (%s,%s,%s,%s) as new
            on duplicate key update
            제목 = new.제목,링크=new.링크;
        """
        args = df.values.tolist()
        mycursor.executemany(query,args)
        con.commit()
        print(f'[{biz_day}] [google_news] {len(df)}개 DB INSERT 성공')
        con.close()
        
if __name__ == '__main__':
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    data = google_stocknews.get_search_google(db_info,biz_day)
    # google_stocknews.insertDB(biz_day,data,db_info)
    # print(data)