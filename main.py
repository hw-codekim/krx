from krx_base_info import load_krx_base_info
# from dart_code_corp import dart_employee
from biz_day import date_biz_day

biz_day = date_biz_day()


# krx_base_info Load --> aws db insert
data = load_krx_base_info.base_info(biz_day)
load_krx_base_info.insertDB(biz_day,data)




#1. dart_code
#2. krx_base_info
#3. krx_daily_price
#4. whynot_report
#5. krx-trade_amount
#6. krx-trade_amount
