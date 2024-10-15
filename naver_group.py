import requests
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import numpy as np
import re
import time
from biz_day import date_biz_day


class naver_group:
    def naver_upjong(biz_day):
        upjong_df = pd.DataFrame() # 그룹
        url = f'https://finance.naver.com/sise/sise_group.naver?type=upjong'
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        posts = soup.find('table',{'class':'type_1'}).find_all('td')
        for post in posts:
            link = 'https://finance.naver.com'
            upjong_name = post.find('a')
        
            if upjong_name:
                # 업종명 추출
                upjong_name = upjong_name.get_text().strip()
                
                # 업종 링크 추출
                upjong_link = post.find('a')['href']
                sub_url = link + upjong_link
                sub_res = requests.get(sub_url)
                sub_soup = BeautifulSoup(sub_res.text,'html.parser')
                sub_data = sub_soup.find('table',{'class':'type_5'})
                
                sub_make = parser.make2d(sub_data)
                sub_table = pd.DataFrame(sub_make,columns=sub_make[0]).drop(index=0)
                
                sub_table = sub_table[['종목명', '현재가', '등락률','거래량', '거래대금','전일거래량']]
                sub_table.replace('', None, inplace=True)
                sub_table.dropna(how='all', inplace=True)  # 모든 컬럼이 NaN 또는 None인 행 삭제
                sub_table['종목명'] = sub_table['종목명'].str.replace('*', '', regex=False).str.strip()
                sub_table.dropna(inplace=True)
                sub_table.insert(0,'그룹',upjong_name)
                sub_table.insert(0,'기준일',biz_day)
                sub_table['현재가'] = sub_table['현재가'].str.replace(',','').astype(int)
                sub_table['등락률'] = sub_table['등락률'].str.replace('%','').str.replace('+','').astype(float)
                sub_table['거래량'] = sub_table['거래량'].str.replace(',','').astype(int)
                sub_table['거래대금'] = sub_table['거래대금'].str.replace(',','').astype(int)
                sub_table['거래대금'] = round(sub_table['거래대금']/100,1)
                sub_table['전일거래량'] = sub_table['전일거래량'].str.replace(',','').astype(int)
                upjong_df = pd.concat([upjong_df,sub_table],ignore_index=True)
                time.sleep(1) 
        print(f'[{biz_day}] [naver_upjong] {len(upjong_df)}개 로딩 성공')  
        return upjong_df

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
                insert into naver_group (기준일,그룹,종목명,현재가,등락률,거래량,거래대금,전일거래량)
                values (%s,%s,%s,%s,%s,%s,%s,%s) as new
                on duplicate key update
                현재가 = new.현재가,등락률=new.등락률,거래량=new.거래량,거래대금=new.거래대금,전일거래량=new.전일거래량;
            """
            args = df.values.tolist()
            mycursor.executemany(query,args)
            con.commit()
            print(f'[{biz_day}] [naver_group] {len(df)}개 DB INSERT 성공')
            con.close()

    
# if __name__ == '__main__':
#     biz_day = date_biz_day()
#     # dd = naver_theme(biz_day)
#     da = naver_upjong(biz_day)
#     print(da.info())