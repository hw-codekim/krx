import warnings
from datetime import datetime,timedelta
import time
import pandas as pd
import OpenDartReader

from biz_day import date_biz_day
from key.dart_conn import dartKey
from key.db_info import connectDB

# from dart_stock_buysell import stock_buysell
from dart_list import dartList
from krx_base_info import krx_base_info
from krx_daily_price import krx_daily_price
from krx_trade_amount import krx_trade_amount
from krx_value import krx_value
from whynot_report import whynot_report

warnings.filterwarnings("ignore")


def krxBaseInfo(biz_day,db_info):
    df = krx_base_info.base_info(biz_day)
    krx_base_info.insertDB(biz_day,df,db_info)

def krxDailyPrice(biz_day,db_info):
    df = krx_daily_price.daily_price(biz_day)
    krx_daily_price.insertDB(biz_day,df,db_info)

def krxTradeAmount(biz_day,db_info):
    df = krx_trade_amount.corp_trading(biz_day,db_info)
    krx_trade_amount.insertDB(biz_day,df,db_info)

def krxValue(biz_day,db_info):
    df = krx_value.daily_value(biz_day)
    krx_value.insertDB(biz_day,df,db_info)

    
def whynotReport(biz_day,db_info):
    df = whynot_report.whynot_report(biz_day)
    whynot_report.insertDB(biz_day,df,db_info)

# def dartStockBuysell(biz_day,dart,dart_key,db_info):
#     bgn_de = biz_day
#     end_de = biz_day
#     page_count = 100
#     search = ['임원ㆍ주요주주특정증권등소유상황보고서']
    
#     list_df = pd.DataFrame()
#     for page_no in range(1,10):
#         lists = dartList.loadDart_list(dart_key,bgn_de,end_de,page_no,page_count,search)
#         list_df = pd.concat([list_df,lists])
#         time.sleep(1)
        
#     df = pd.DataFrame()
#     for rcept_no,corp_name,flr_nm,rcept_dt  in zip(list_df['rcept_no'],list_df['corp_name'],list_df['flr_nm'],list_df['rcept_dt']):
#         dd= stock_buysell.stock_buysell(dart,rcept_no,corp_name,flr_nm,rcept_dt)
#         df = pd.concat([df,dd])
#     stock_buysell.db_insert_stock_buysell(df,db_info)




#1. dart_code
#2. krx_base_info
#3. krx_daily_price
#4. whynot_report
#5. krx-trade_amount


if __name__ == '__main__':
    
    #기본적인 데이터
    dart_key = dartKey.dart_key()
    dart = OpenDartReader(dart_key)
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    
    # 매일 데이터 수집 
    krxBaseInfo(biz_day,db_info)         # 1. krx 표준코드 있는 테이블
    krxDailyPrice(biz_day,db_info)       # 2. krx 매일 등락률 및 시총
    krxTradeAmount(biz_day,db_info)      # 3. 시총 5000억 이상 종목만 거래대금 DB INSERT 20~30분 소요
    krxValue(biz_day,db_info)            # 4. krx EPS,PER.. 등 업데이트
    whynotReport(biz_day,db_info)        # 4. whynotsell 사이트의 레포트를 DB Insert 
    
    
    
    
    # dartStockBuysell(biz_day,dart,dart_key,db_info)
    
    
    
    # b_day = datetime.strptime(biz_day,'%Y%m%d')
    # # biz_day = (b_day - timedelta(days=2)).strftime('%Y%m%d')
    # # print(biz_day)
    
    # for i in range(4,5):
    #     biz_day = (b_day - timedelta(days=i)).strftime('%Y%m%d')
    #     print(biz_day)
    #     try:
    #         dartStockBuysell(biz_day,dart,dart_key,db_info)
    #     except Exception as e:
    #         print('에러발생_스킵:',e)
    
    


#1. dart_code
#2. krx_base_info
#3. krx_daily_price
#4. whynot_report
#5. krx-trade_amount
#6. krx-trade_amount
