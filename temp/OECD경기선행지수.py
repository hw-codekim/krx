import requests
import pandas as pd
from bs4 import BeautifulSoup
from html_table_parser import parser_functions

# OECD 12개 나라 경기선행지수 가져오기
url = 'https://stats.oecd.org/index.aspx?queryid=6617' 
res = requests.get(url)
soup = BeautifulSoup(res.text,'html.parser')
data = soup.find('table',{'class':'DataTable'})
table = parser_functions.make2d(data)
df = pd.DataFrame(data = table[7:],columns=table[3])
df = df.apply(pd.to_numeric,errors='ignore')
df = df[['Country','Australia', 'Canada', 'France', 'Germany','Italy', 'Japan', 'Korea', 'Mexico',
       'United Kingdom', 'United States',"China (People's Republic of)",'India' ]]

# 엑셀 저장하고 표로 만들기
writer = pd.ExcelWriter('OECD경기선행지수.xlsx',engine='xlsxwriter')
df.to_excel(writer,sheet_name='Sheet1',index=False)

workbook = writer.book
worksheet = writer.sheets['Sheet1']

country = {'I':'B2',
           'H':'H2',
           'M':'N2',
           'L':'T2',
           'D':'B14',
           'E':'H14',
           'F':'N14',
           'N':'T14',
           }


for x,k in country.items():
# 한국
    chart = workbook.add_chart({'type':'column'})
    custom_labels = [{'delete':True} for i in range(23)] # 막대그래프 라벨은 마지막만 표기
    chart.add_series({
        "categories": "=Sheet1!$A$2:$A$25", #x축 
        'values':f'=Sheet1!${x}$2:${x}$25', # 값을 막대 그래프로
        'name':f'=Sheet1!${x}$1', # 차트 제목
        'name_fond':14, #
        
        # 'trendline':{'type':'moving_average','period':2}, # 롤링 기능
        'data_labels':{'value':True,'custom': custom_labels},# 막대그래프 라벨은 마지막만 표기
        'gap':10,
        })
    # chart.set_x_axis({'position': 'none'})
    chart.set_legend({'position':'none'})
    chart.set_x_axis({
        'num_font': {'size': 9},
    })
    chart.set_y_axis({
        'label_position': 'none', #y축 라벨
        'major_gridlines': { #x축 가이드라인
            'visible': False,
        },
    })
    worksheet.insert_chart(k,chart,{'x_scale':0.79,'y_scale':0.8})

writer.close()