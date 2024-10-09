from sqlalchemy import create_engine
import json
import requests
from io import BytesIO
import zipfile
import xmltodict
import pandas as pd
from key.db_info import connectDB
import pymysql


class dart_code_corp:

    def code_corp(krx_corpList:pd.DataFrame):
        with open("./key/dart_api_key.json",'r',encoding='utf-8') as f:
            key = json.load(f)   

        dart_key = key['dart_api_key']
        codezip_url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={dart_key}'
        codezip_data = requests.get(codezip_url)
        codezip_file = zipfile.ZipFile(BytesIO(codezip_data.content))
        codecorp_xml = codezip_file.read('CORPCODE.xml').decode('utf-8')
        codecorp_odict = xmltodict.parse(codecorp_xml)
        codecorp_dict = json.loads(json.dumps(codecorp_odict))
        codecorp_data = codecorp_dict.get('result').get('list')
        codecorp_list = pd.DataFrame(codecorp_data) 
        dart_corp_list = codecorp_list[~codecorp_list.stock_code.isin([None])].reset_index(drop=True) # None 제외
        merged_corp_list = dart_corp_list.merge(krx_corpList, left_on='stock_code', right_on='종목코드', how='inner')
        merged_corp_list = merged_corp_list[['corp_code','corp_name','stock_code','modify_date']]
        return merged_corp_list
        
        
    def corp_list(db_info):
        con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
        mycursor = con.cursor()
        sql = f"""
            SELECT 종목코드,종목명 FROM krx_base_info;
            """
        mycursor.execute(sql)
        result = mycursor.fetchall()
        con.close()
        return result 
       
    def insert_db(db_info,merged_corp_list):
        db_info = connectDB.db_conn()
        engine = f'mysql+pymysql://{db_info[0]}:{db_info[1]}@{db_info[2]}:{int(db_info[3])}/{db_info[4]}'
        merged_corp_list.to_sql(name='dart_code',con = engine,index=False,if_exists='append')
    
if __name__ == '__main__':
    db_info = connectDB.db_conn()
    krx_corpList= dart_code_corp.corp_list(db_info)
    krx_corpList = pd.DataFrame(krx_corpList,columns=['종목코드','종목명'])
    
    merged_corp_list = dart_code_corp.code_corp(krx_corpList)
    dart_code_corp.insert_db(db_info,merged_corp_list)
