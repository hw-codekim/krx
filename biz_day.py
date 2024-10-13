import requests
from bs4 import BeautifulSoup
import re
import collections

if not hasattr(collections, 'Callable'):
    collections.Callable = collections.abc.Callable
    
def date_biz_day():
    url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')

    parse_day = soup.select_one('#time').text
    
    biz_day = re.findall('[0-9]+',parse_day)
    biz_day = ''.join(biz_day)
    return biz_day

# if __name__ == '__main__':
#     date_biz_day()