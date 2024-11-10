import requests
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
import numpy as np
import re
import time
from biz_day import date_biz_day
from key.db_info import connectDB


def naver_maxmin(biz,code,name):
    lists = []
    url = f'https://finance.naver.com/item/main.naver?code={code}'
    res  = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')
    cur = soup.select_one('#content > div.section.invest_trend > div.sub_section.right > table > tbody > tr:nth-child(2) > td:nth-child(2) > em').text
    max = soup.select_one('table.rwidth tr:nth-of-type(2) > td > em:nth-of-type(1)').text
    min = soup.select_one('table.rwidth tr:nth-of-type(2) > td > em:nth-of-type(2)').text
    lists.append([cur,max,min])
    df = pd.DataFrame(lists,columns=['현재가','52최고가','52최저가'])
    df = df.apply(lambda x : x.str.replace(',','').astype(int))
    df['신고가율'] = round((df['현재가']/df['52최고가'])*100,1)
    df['갭'] = round((df['52최저가']/df['52최고가'])*100,1)
    df.insert(0,'종목명',name)
    df.insert(0,'기준일',biz)
    return df

def loadDB_dart_code(db_info):
    con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
    mycursor = con.cursor()
    sql = f"""
        SELECT stock_code,corp_name FROM dart_code
        """
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    return result

if __name__ == '__main__':
    db_info = connectDB.db_conn()
    biz = date_biz_day()
    corp_lists = loadDB_dart_code(db_info)
    print(corp_lists)
    code = '365330'
    name = '에스와이스틸텍'
    dd = naver_maxmin(biz,code,name)
    print(dd)