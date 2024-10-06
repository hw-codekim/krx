import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
import pymysql
import time
import random
from tqdm import tqdm
import warnings
import numpy as np
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


def corp_trading_money(corp):
    con = pymysql.connect(user='root',
                      passwd='dkvkxm8093!',
                      host = '127.0.0.1',
                      db='stock',
                      charset='utf8'                      
                      )
    mycursor = con.cursor()
    sql = f"""
        SELECT 기준일,종목코드,종목명,투신,사모,연기금등,외국인 FROM trade_amount
        where 1=1
        AND 종목명 = '{corp}';
        """
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    return result

df =corp_trading_money('프로텍')
dd = pd.DataFrame(df,columns=['기준일','종목코드','종목명','투신','사모','연기금등','외국인'])


# 한글 폰트 설정 (Windows 환경에서는 'Malgun Gothic'을 사용할 수 있습니다)
plt.rc('font', family='Malgun Gothic')

# 음수 부호가 깨지는 문제 해결
plt.rcParams['axes.unicode_minus'] = False
# '기관' 컬럼을 추가 (투신 + 사모 + 연기금등)
dd['기관'] = dd['투신'] + dd['사모'] + dd['연기금등']

# 5일 이동평균을 구하기 위해 rolling 적용 (원하는 기간으로 window 설정 가능)
dd['기관5'] = dd['기관'].rolling(window=5).mean()
dd['외국인5'] = dd['외국인'].rolling(window=5).mean()

# 그래프 스타일 설정
plt.style.use('seaborn-darkgrid')

# 그래프 크기 설정
plt.figure(figsize=(10, 6))

# 꺾은선 그래프 그리기
plt.plot(dd['기준일'], dd['기관5'], label='기관 5일 평균', color='blue', linestyle='-', linewidth=2)
plt.plot(dd['기준일'], dd['외국인5'], label='외국인 5일 평균', color='orange', linestyle='-', linewidth=2)


# 기관의 최대값과 최소값 계산
기관_max = dd['기관5'].max()
기관_min = dd['기관5'].min()

# 최대값에 가로선
plt.axhline(기관_max, color='black', linestyle='-', linewidth=0.5, label='기관 최대값')

# 최소값에 가로선
plt.axhline(기관_min, color='black', linestyle='-', linewidth=0.5, label='기관 최소값')

# 그래프 제목 및 축 레이블 설정
plt.title('기관 순매수 5일 평균 변화', fontsize=16)
plt.xlabel('날짜', fontsize=12)
plt.ylabel('순매수 금액', fontsize=12)

# 범례 표시
plt.legend(loc='best')

# 그래프 보여주기
plt.show()
# for i in df:
#     print(i)
