import requests
import json
import pymysql
import pandas as pd
import time
from tqdm import tqdm
from key.db_info import connectDB
import requests
from bs4 import BeautifulSoup
import re

def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.content)

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day

def dart_key():
    with open("./key/dart_api_key.json",'r',encoding='utf-8') as f:
        key = json.load(f)   

    dart_key = key['dart_api_key']
    return dart_key

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
    return df


if __name__ == '__main__':
    date = date_biz_day()
    dartkey = dart_key()
    bgn_de = date
    end_de = date
    page_no = 1
    page_count = 100
    search = ['임원ㆍ주요주주특정증권등소유상황보고서']
    
    
    lists = loadDart_list(dartkey,bgn_de,end_de,page_no,page_count,search)
    print(lists)
