import pandas as pd
from bs4 import BeautifulSoup
import requests
from html_table_parser import parser_functions as parser
import os
os.chdir('C:\투자기록\정리\코딩\반도체가격')
# DRAM Spot Price

def DRAM_Spot_Price():
    url = 'https://www.dramexchange.com/'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    data = soup.select('#tb_NationalDramSpotPrice')
    p = parser.make2d(data[0]) #표로 만들어줌
    data = pd.DataFrame(p) # 필요한부분만 불러옴
    data.columns = data.iloc[0]
    data.drop(0,inplace=True)
    date = soup.select_one('#NationalDramSpotPrice_show_day > span')
    date= date.text
    date = date[13:33]
    data['Update'] = date
    # data['unit'] = 'DRAM_Spot_Price'
    data.insert(0,'unit','DRAM_Spot_Price')
    data.apply(pd.to_numeric,errors='ignore')
    return data

# Flash Spot Price
def Flash_Spot_Price():
    url = 'https://www.dramexchange.com/'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    data = soup.select('#tb_NationalFlashSpotPrice')
    p = parser.make2d(data[0]) #표로 만들어줌
    data = pd.DataFrame(p) # 필요한부분만 불러옴
    data.columns = data.iloc[0]
    data.drop(0,inplace=True)
    date = soup.select_one('#NationalFlashSpotPrice_show_day > span')
    date= date.text
    date = date[13:33]
    data['Update'] = date
    data.insert(0,'unit','Flash_Spot_Price')
    data.apply(pd.to_numeric,errors='ignore')
    return data
    

df = pd.DataFrame()
for i in range(1):
    a = DRAM_Spot_Price()
    b = Flash_Spot_Price()
    df = pd.concat([df,a,b])

df = df[df['Item'].str.contains('1Gx16|MLC 64Gb 8GBx8')]
df['Session Change'] = df['Session Change'].str.replace('%','',regex=True)
df = df.apply(pd.to_numeric,errors='ignore')

data = pd.read_excel('DRAMeXchange_Semi_price.xlsx',sheet_name=0,index_col=0)
merge = pd.concat([data,df])
merge.to_excel('DRAMeXchange_Semi_price.xlsx',index_label=False)


