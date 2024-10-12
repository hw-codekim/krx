from biz_day import date_biz_day
import pandas as pd
from key.dart_conn import dartKey
from key.db_info import connectDB
import OpenDartReader
from dart_stock_buysell import stock_buysell
from dart_list import dartList
import warnings
from datetime import datetime,timedelta
import time
warnings.filterwarnings("ignore")


def dartStockBuysell(biz_day,dart,dart_key,db_info):
    bgn_de = biz_day
    end_de = biz_day
    page_count = 100
    search = ['임원ㆍ주요주주특정증권등소유상황보고서']
    
    list_df = pd.DataFrame()
    for page_no in range(1,10):
        lists = dartList.loadDart_list(dart_key,bgn_de,end_de,page_no,page_count,search)
        list_df = pd.concat([list_df,lists])
        time.sleep(1)
        
    df = pd.DataFrame()
    for rcept_no,corp_name,flr_nm,rcept_dt  in zip(list_df['rcept_no'],list_df['corp_name'],list_df['flr_nm'],list_df['rcept_dt']):
        dd= stock_buysell.stock_buysell(dart,rcept_no,corp_name,flr_nm,rcept_dt)
        df = pd.concat([df,dd])
    stock_buysell.db_insert_stock_buysell(df,db_info)






if __name__ == '__main__':
    dart_key = dartKey.dart_key()
    dart = OpenDartReader(dart_key)
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    
    b_day = datetime.strptime(biz_day,'%Y%m%d')
    # biz_day = (b_day - timedelta(days=2)).strftime('%Y%m%d')
    # print(biz_day)
    
    for i in range(4,5):
        biz_day = (b_day - timedelta(days=i)).strftime('%Y%m%d')
        print(biz_day)
        try:
            dartStockBuysell(biz_day,dart,dart_key,db_info)
        except Exception as e:
            print('에러발생_스킵:',e)
    
    


#1. dart_code
#2. krx_base_info
#3. krx_daily_price
#4. whynot_report
#5. krx-trade_amount
#6. krx-trade_amount
