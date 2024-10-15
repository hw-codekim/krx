import requests
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import numpy as np
import re
import time
from biz_day import date_biz_day

class naver_theme:
    def naver_theme(biz_day):

        df_theme_stock = pd.DataFrame() # 테마 안에 종목들 리스트
        
        for i in range(1,10):
            
            try:
                url = f'https://finance.naver.com/sise/theme.naver?&page={i}'
                res = requests.get(url)
                soup = BeautifulSoup(res.text, 'html.parser')
                posts = soup.select('td.col_type1')
                print(f'{i}페이지 성공')
                for post in posts:
                    link = 'https://finance.naver.com'
                    theme_name = post.find('a').get_text()
                    theme_link = post.find('a')['href']

                    sub_url = link + theme_link
                    sub_res = requests.get(sub_url)
                    sub_soup = BeautifulSoup(sub_res.text,'html.parser')
                    sub_data = sub_soup.find('table',{'class':'type_5'})
                    sub_make = parser.make2d(sub_data)
                    sub_table = pd.DataFrame(sub_make,columns=sub_make[0]).drop(index=0)
                    sub_table = sub_table[['종목명', '현재가', '등락률','거래량', '거래대금','전일거래량']]
                    
                    sub_table.columns = ['종목명','테마사유','현재가', '등락률','거래량', '거래대금','전일거래량']
                    sub_table.replace('', None, inplace=True)
                    sub_table.dropna(how='all', inplace=True)  # 모든 컬럼이 NaN 또는 None인 행 삭제
                    sub_table['종목명'] = sub_table['종목명'].str.replace('*', '', regex=False).str.strip()
                    sub_table['테마사유'] = sub_table.apply(lambda row: (row['테마사유'].replace(f'테마 편입 사유\n\n\n{row["종목명"]}\n', '')[:70]), axis=1)
                    sub_table.dropna(inplace=True)
                    sub_table.insert(0,'테마',theme_name)
                    sub_table.insert(0,'기준일',biz_day)
                    sub_table['현재가'] = sub_table['현재가'].str.replace(',','').astype(int)
                    sub_table['등락률'] = sub_table['등락률'].str.replace('%','').str.replace('+','').astype(float)
                    sub_table['거래량'] = sub_table['거래량'].str.replace(',','').astype(int)
                    sub_table['거래대금'] = sub_table['거래대금'].str.replace(',','').astype(int)
                    sub_table['거래대금'] = round(sub_table['거래대금']/100,1)
                    sub_table['전일거래량'] = sub_table['전일거래량'].str.replace(',','').astype(int)
                    df_theme_stock = pd.concat([df_theme_stock,sub_table],ignore_index=True)
                    time.sleep(1)
            except Exception as e:
                print(f'{i} 페이지 실패',e)
        print(f'[{biz_day}] [naver_theme] {len(df_theme_stock)}개 로딩 성공')  
        return df_theme_stock


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
                insert into naver_theme (기준일,테마,종목명,테마사유,현재가,등락률,거래량,거래대금,전일거래량)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
                on duplicate key update
                테마사유 = new.테마사유,현재가 = new.현재가,등락률=new.등락률,거래량=new.거래량,거래대금=new.거래대금,전일거래량=new.전일거래량;
            """
            args = df.values.tolist()
            mycursor.executemany(query,args)
            con.commit()
            print(f'[{biz_day}] [naver_theme] {len(df)}개 DB INSERT 성공')
            con.close()