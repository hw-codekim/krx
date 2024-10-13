# 사이트,기준일,제목,URL

import requests
from bs4 import BeautifulSoup
import pandas as pd

def crawl_bizwatch():
    url = 'http://news.bizwatch.co.kr/search/news/1'
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    
    posts = soup.select('.news_list')
    
    
    for post in posts:
        # print(post)
        # date = post.select_one('span.date')

        title = post.select_one('#search > section.left_content > div.all_news > ul > li:nth-child(1) > dl > dt > a').get_text()
        print(title)
        # link_temp = post.select_one(' div.content-wrapper >ul>li >article > div:nth-of-type(2) > a').attrs['href']
        # link = 'https://themiilk.com' + link_temp
        # print(date)
    return 

if __name__ == '__main__':
    dd = crawl_bizwatch()
    print(dd)