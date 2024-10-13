import warnings
from datetime import datetime,timedelta
import time
import pandas as pd
import OpenDartReader

from biz_day import date_biz_day
from key.dart_conn import dartKey
from key.db_info import connectDB

from dart_stock_buysell import dart_stock_buysell
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

def dartStockBuysell(biz_day,dart,dart_key,db_info):
    df = dart_stock_buysell.stock_buysell(dart_key,dart,biz_day)
    dart_stock_buysell.insertDB(biz_day,df,db_info)

if __name__ == '__main__':
    
    #기본적인 데이터
    dart_key = dartKey.get_dart_key()
    dart = OpenDartReader(dart_key)
    biz_day = date_biz_day()
    db_info = connectDB.db_conn()
    
    # 매일 데이터 수집 
    krxBaseInfo(biz_day,db_info)                         # 1. krx 표준코드 있는 테이블
    krxDailyPrice(biz_day,db_info)                       # 2. krx 매일 등락률 및 시총
    krxValue(biz_day,db_info)                            # 3. krx EPS,PER.. 등 업데이트
    whynotReport(biz_day,db_info)                        # 4. whynotsell 사이트의 레포트를 DB Insert 
    krxTradeAmount(biz_day,db_info)                      # 6. 시총 5000억 이상 종목만 거래대금 DB INSERT 20~30분 소요
    dartStockBuysell(biz_day,dart,dart_key,db_info)      # 5. 임원ㆍ주요주주특정증권등소유상황보고서 항목 DB INSERT
    
    
    # 가져오기
    