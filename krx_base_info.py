import requests
from io import BytesIO
import pandas as pd
import numpy as np
import pymysql
from key.db_info import connectDB
from biz_day import date_biz_day

class load_krx_base_info():
        
        biz_day = date_biz_day()
        def base_info(biz_day):
                try:
                        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
                        gen_otp = {
                                'locale': 'ko_KR',
                                'mktId': 'ALL',
                                'share': '1',
                                'csvxls_isNo': 'false',
                                'name': 'fileDown',
                                'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
                                }

                        headers = {
                                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                                }

                        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

                        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
                        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
                        corp_code = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
                        corp_code.columns = corp_code.columns.str.replace(' ','')
                        corp_code = corp_code[['표준코드','단축코드','한글종목약명','상장일','시장구분','주식종류']]
                        corp_code.columns = ['표준코드','종목코드','종목명','상장일','시장구분','종목구분']
                        corp_code = corp_code.replace({np.nan:None})
                        print(f'{biz_day} krx_base_info 로딩 성공')
                except Exception as e:
                        print(e)
                return corp_code
                
        def insertDB(biz_day,data):
                try:
                        db_info = connectDB.db_conn()
                        con = pymysql.connect(
                                user=db_info[0],
                                password=db_info[1],
                                host = db_info[2],
                                port = int(db_info[3]),
                                database=db_info[4],                     
                                )
                        mycursor = con.cursor()
                        query = f"""
                        insert into krx_base_info (표준코드,종목코드,종목명,상장일,시장구분,종목구분)
                        values (%s,%s,%s,%s,%s,%s) as new
                        on duplicate key update
                        종목명 = new.종목명,상장일=new.상장일,시장구분=new.시장구분,종목구분=new.종목구분;
                        """
                        args = data.values.tolist()
                        mycursor.executemany(query,args)
                        con.commit()
                        con.close()
                        print(f'{biz_day} krx_base_info AWS_DB Insert 성공')
                except Exception as e:
                        print(e)
                        
if __name__ == '__main__':
        data = load_krx_base_info.base_info()
        load_krx_base_info.insertDB(data)