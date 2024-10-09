from datetime import datetime,timedelta
import pandas as pd
import xlwings as xw
import FinanceDataReader as fdr
import os

os.chdir('C:\\')

startday = (datetime.today()-timedelta(3)).strftime('%Y-%m-%d')
endday = (datetime.today()).strftime('%Y-%m-%d')



#전체 종목명, 종목코드
def TotalStock():
    df_krx = fdr.StockListing('krx')
    word = ('우','우B','우C','리츠','스팩','호')
    df_krx = df_krx[~df_krx['Name'].str.endswith(word)]
    df_krx = df_krx[~df_krx['Market'].str.contains('KONEX')]
    df_krx = df_krx[~df_krx['Dept'].str.contains('관리|환기')]
    df_krx = df_krx[['Name','Code']]
    # df_krx = df_krx.head(10)
    stocklists = df_krx.set_index('Name').to_dict()
    stocklists = stocklists['Code']
    return stocklists

# 갭상승 후 양봉 마무리
def dailyDatareader(stockCode,startday,endday):
    krx_datareader = fdr.DataReader(stockCode,startday,endday)
    krx_datareader['Close1'] = krx_datareader['Close'].shift(1)
    krx_datareader['gap']=(((krx_datareader['Open']-krx_datareader['Close1'])/krx_datareader['Close1'])*100).round(1)
    krx_datareader['Change'] = round(krx_datareader['Change']*100,1)
    return krx_datareader['gap'][-1], krx_datareader['Change'][-1]

def smallCap_1000under_10pro_up():
    df_krx = fdr.StockListing('krx')
    df_krx['Marcap'] = round(df_krx['Marcap']/100000000,0).astype('int')
    df_krx['Amount'] = round(df_krx['Amount']/100000000,0).astype('int')
    df_krx['비중'] = ((df_krx['Amount']/df_krx['Marcap'])*100).astype('int')
    df_krx = df_krx[df_krx['Marcap'] < 2000]
    word = ('우','우B','우C','리츠','스팩','호')
    df_krx = df_krx[~df_krx['Name'].str.endswith(word)]
    df_krx = df_krx[~df_krx['Market'].str.contains('KONEX')]
    df_krx = df_krx[~df_krx['Dept'].str.contains('관리|환기')]
    df_krx = df_krx[df_krx['ChagesRatio'] > 10]
    df_krx = df_krx[(df_krx['Amount'] > 100) | (df_krx['Volume'] > 10000000) ]
    df_krx.insert(0,'날짜',endday)
    df_krx = df_krx[['날짜', 'Name', 'Market', 'Close', 'ChagesRatio','Amount','Marcap', 'Stocks', '비중']]
    df_krx = df_krx.reset_index().drop(columns='index')
    return df_krx 

# 엑셀 save
def excel_Save(tabledata):
    path = 'C:\stockDaily.xlsm'
    wb = xw.Book(path)
    ws = wb.sheets('gap상승')
    lastRow = ws.range('B3').end('down')
    ws.range(lastRow).value = tabledata
    return


dailygapUp={}
stockLists = TotalStock()
for k,v in stockLists.items():
    lists = dailyDatareader(v,startday,endday)
    dailygapUp[k] = lists

tabledata = pd.DataFrame.from_dict(dailygapUp,orient='index',columns=['gap','dailyrate'])
tabledata = tabledata[tabledata['gap'] > 1]
tabledata['Diff'] = tabledata['dailyrate']- tabledata['gap']
tabledata = tabledata[tabledata['Diff'] > 0]
tabledata.insert(0,'날짜',endday)

excel_Save(tabledata)  