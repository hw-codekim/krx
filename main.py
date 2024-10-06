from db.krx_base_info import load_krx_base_info
from db.dart_employee import dart_employee
from biz_day import date_biz_day

biz_day = date_biz_day()


# krx_base_info Load --> aws db insert
data = load_krx_base_info.base_info(biz_day)
load_krx_base_info.insertDB(biz_day,data)

