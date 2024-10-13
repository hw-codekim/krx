## https://www.ksia.or.kr/bbs/board.php?bo_table=dailyNews 
# 반도체협회 데일리 기사 가져옴

import requests
import urllib3
from bs4 import BeautifulSoup
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def semi_news():
    news_list = []
    for k in range(1,3):
        url = f'https://www.ksia.or.kr/bbs/board.php?bo_table=dailyNews&page={k}'
        res = requests.get(url,verify=False)
        soup = BeautifulSoup(res.text,'html.parser')
        posts = soup.select('#fboardlist > div > table > tbody >tr')
        for i,post in enumerate(posts):
            date = post.select_one(f'tr:nth-child({i+1}) > td.td_date.KSIA-rt-border00').text
  
            if date == '2024-10-10':
                title = post.select_one(f'tr:nth-child({i+1}) > td.td_subject.KSIA-rt-border00 > a').text.strip()
                link = post.select_one(f'tr:nth-child({i+1}) > td.td_subject.KSIA-rt-border00 > a').attrs['href']            
                news_list.append([date,title,link])
      
            
    news_table = pd.DataFrame(news_list,columns =['날짜','제목','링크'])
    return news_table

semi_df = semi_news()

unique_df = semi_df['날짜'].unique()[0]
final_df = semi_df[semi_df['날짜'] == unique_df]

semi_df.to_excel(f'./semi_news_{unique_df}.xlsx',index=False)