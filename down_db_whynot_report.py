
import pymysql
import warnings
from key.db_info import connectDB
import pandas as pd
warnings.filterwarnings("ignore")


def down_whynot_report(db_info):
    con = pymysql.connect(
            user=db_info[0],
            password=db_info[1],
            host = db_info[2],
            port = int(db_info[3]),
            database=db_info[4],                     
            )
    mycursor = con.cursor()
    sql = """
        select * from whynot_report
        """
    mycursor.execute(sql)
    result = mycursor.fetchall()
    con.close()
    return result

if __name__ == '__main__':
    db_info = connectDB.db_conn()
    data = down_whynot_report(db_info)
    df = pd.DataFrame(data,columns=['id','date','company_name','analyst_name','target_price','judge','title','description','analyst_rank','stock_code_id','analyst_id'])
    df.to_excel('whynot_report_20240101_20241008.xlsx',index=False)