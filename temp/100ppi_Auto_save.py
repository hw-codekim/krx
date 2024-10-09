
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen 
from urllib.parse import quote_plus #한글을 아스키코드로 변환
import xlwings as xw
from PIL import Image
import os
from datetime import date
import glob

#100ppi.com 의 원자재 정보 이미지 다운로드
def material_100ppi(product_no):
    url = f'https://www.100ppi.com/vane/detail-{product_no}.html'

    html = urlopen(url).read()
    soup=BeautifulSoup(html,'html.parser')
    img = soup.select_one('body > div:nth-child(4) > div:nth-child(2) > div.mainleft-a > div:nth-child(2) > img')
    with urlopen(img['src']) as f:
        with open(f'./100ppi/{product_no}'+'.jpg','wb') as h:
            a = f.read()
            h.write(a)
            
lists = {
    959 : '유리',
    368 : '가성소다',
    975:'MDI',
    1095:'TDI',
    463:'폴리실리콘',
    388:'부타디엔 고무',
    586:'천연고무',
    524:'구리',
    492:'주석',
    634:'스테인레스강판',
    961:'철광석(호주)',
    927:'철근',
    603:'시멘트',
    312:'대두박',
    769:'콩',
    820:'팜유',
    274:'옥수수',
    564:'백설탕',
    482:'알루미늄',
    976:'폴리에스테르(섬유원단)',
    897:'LNG',
    1036:'WTI',
    123:'불산',
    
}

for x,v in lists.items():
    try:
        material_100ppi(x)
    except Exception as e:
        print(e)

# 엑셀에 이미지 파일 저장하기
today = (date.today()).strftime("%Y%m%d") #오늘

wb = xw.Book(glob.glob('./100ppi*.xlxl'))
ws = wb.sheets.add('100ppi')
ws.activate()

a = 0
for x,v in lists.items():
    pos = os.path.abspath(f'./100ppi/{x}.jpg')
    print(a)
    for i in range(a,len(lists)):   
        ws.pictures.add(
        pos, # 절대경로
        top = ws.range(f'C{2+i}').top+10,
        left = ws.range(f'C{2+i}').left+10,
        width = 490/1.5,
        height = 300/1.5,
        )
        ws.range(f'C{2+i}').column_width = 57
        ws.range(f'C{2+i}').row_height = 220
        ws.range(f'B{2+i}').value=list(lists.values())[i]
        ws.range(f'B{2+i}').column_width = 13    
        break
    a = a + 1
wb.save(f'./100ppi_{today}.xlsx')
wb.close()