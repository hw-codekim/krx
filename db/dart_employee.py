from sqlalchemy import create_engine
import json
import requests
from io import BytesIO
import zipfile
import xmltodict
import pandas as pd
from db_info import connectDB


class dart_employee:
    def code_corp():
        with open("./dart_api_key.json",'r',encoding='utf-8') as f:
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
        corp_list = codecorp_list[~codecorp_list.stock_code.isin([None])].reset_index(drop=True) # None 제외
        print(corp_list.info())

        db_info = connectDB.db_conn()

        engine = f'mysql+pymysql://{db_info[0]}:{db_info[1]}@{db_info[2]}:{int(db_info[3])}/{db_info[4]}'
        corp_list.to_sql(name='dart_code',con = engine,index=True,if_exists='append')

if __name__ == '__main__':
    main()
    dart_employee.code_corp()