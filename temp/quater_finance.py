import OpenDartReader
import pandas as pd
from datetime import datetime
import time
import warnings

warnings.simplefilter("ignore")
api_key = '08d5ae18b24d9a11b7fd67fb0d79c607f1c88464'
dart = OpenDartReader(api_key)
today = datetime.today().strftime("%Y%m%d") #오늘

#code : 종목코드
#corp : 종목이름
#fs_div : 분기 1(11013),2(11012),3(11014),4(11011)
#reprt_code : 연결, 개별 ( OFC, CFS)

def quater_fin(code,corp,year,reprt_code,fs_div):
    try:
        df = dart.finstate_all(code, year,reprt_code=reprt_code,fs_div=fs_div)
        df = df[df['sj_nm'].str.contains('손익')]
        df = df[df['account_id'].str.contains('ifrs-full_Revenue')|df['account_id'].str.contains('OperatingIncomeLoss')]
        df = df[['account_nm','thstrm_amount']]
 

        if reprt_code == '11014':
            df.columns = ['항목',f'{year}.3Q']
            df = df.apply(pd.to_numeric,errors = 'ignore')
            df = df.sort_values(by='항목',ascending=True)
            df['항목'].iloc[0] = '매출액'
            df['항목'].iloc[1] = '영업이익'
            df.insert(0,'기업',corp)
            df = df.set_index(['기업','항목'])
            df = df.apply(lambda x : round(x/100000000,0))
            
            
        elif reprt_code == '11012':
            df.columns = ['항목',f'{year}.2Q']
            df = df.apply(pd.to_numeric,errors = 'ignore')
            df = df.sort_values(by='항목',ascending=True)
            df['항목'].iloc[0] = '매출액'
            df['항목'].iloc[1] = '영업이익'
            df.insert(0,'기업',corp)
            df = df.set_index(['기업','항목'])
            df = df.apply(lambda x : round(x/100000000,0))   
            
            
        elif reprt_code == '11013':
            df.columns = ['항목',f'{year}.1Q']
            df = df.apply(pd.to_numeric,errors = 'ignore')
            df = df.sort_values(by='항목',ascending=True)
            df['항목'].iloc[0] = '매출액'
            df['항목'].iloc[1] = '영업이익'
            df.insert(0,'기업',corp)
            df = df.set_index(['기업','항목'])
            df = df.apply(lambda x : round(x/100000000,0))  
                    
        
        elif reprt_code == '11011':
            df.columns = ['항목',f'{year}.4Q']
            df = df.apply(pd.to_numeric,errors = 'ignore')
            df = df.sort_values(by='항목',ascending=True)
            df['항목'].iloc[0] = '매출액'
            df['항목'].iloc[1] = '영업이익'
            df.insert(0,'기업',corp)
            df = df.set_index(['기업','항목'])
            df = df.apply(lambda x : round(x/100000000,0))    

    except Exception as e:
        print(year,reprt_code,e)
    time.sleep(2)
    return df

def quarter_4_adj(corp, df):
    try:
        # 각 연도별로 4분기 조정을 위해 컬럼이 존재하는지 확인하고 처리
        if '2023.4Q' in df.columns:
            df['2023.4Q'] = df['2023.4Q'] - (df.get('2023.3Q', 0) + df.get('2023.2Q', 0) + df.get('2023.1Q', 0))
        
        if '2022.4Q' in df.columns:
            df['2022.4Q'] = df['2022.4Q'] - (df.get('2022.3Q', 0) + df.get('2022.2Q', 0) + df.get('2022.1Q', 0))
        
        if '2021.4Q' in df.columns:
            df['2021.4Q'] = df['2021.4Q'] - (df.get('2021.3Q', 0) + df.get('2021.2Q', 0) + df.get('2021.1Q', 0))
        
        if '2020.4Q' in df.columns:
            df['2020.4Q'] = df['2020.4Q'] - (df.get('2020.3Q', 0) + df.get('2020.2Q', 0) + df.get('2020.1Q', 0))

        # 데이터프레임 전치
        df = df.T
        df[corp,'opm'] = round(df[corp,'영업이익']/df[corp,'매출액']*100,1)
        df[corp,'yoy'] = round((df[corp,'매출액'] - df[corp,'매출액'].shift(4))/df[corp,'매출액'].shift(4)*100,1)

        # 다시 전치
        df = df.T

    except Exception as e:
        print(f"Error: {e}")
    return df
    


#fs_div : 분기 1(11013),2(11012),3(11014),4(11011)
code = '054210'
corp = '이랜텍'
years = ['2019','2020','2021','2022','2023','2024']
# years = ['2024'] # 평가

reprt_codes = ['11013','11012','11014','11011']
# reprt_codes = ['11013'] # 평가

result_df = pd.DataFrame()
for year in years:
    for reprt_code in reprt_codes:
        try:
            fs_div = 'CFS' # OFS 개별, CFS 연결
            df = quater_fin(code,corp,year,reprt_code,fs_div)
            result_df = pd.concat([result_df,df],axis=1)
            
        except Exception as e:
            try:
                fs_div = 'OFS'  # OFS 개별
                df = quater_fin(code, corp, year, reprt_code, fs_div)
                result_df = pd.concat([result_df,df],axis=1)
                
            except Exception as e:
                print(e)
ttl = quarter_4_adj(corp,result_df)
ttl.to_clipboard()
ttl.to_excel(f'./개별종목_재무/{corp}_{today}.xlsx')

