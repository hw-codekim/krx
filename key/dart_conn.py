import json
import os

# class dartKey:
#     @staticmethod
#     def get_dart_key():
#         # 환경 변수에서 DART_API_KEY 값을 불러옴
#         dart_key = os.getenv('DART_API_KEY')
#         if dart_key is None:
#             raise ValueError("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
#         return dart_key
    
    
class dartKey:
    def get_dart_key():
        with open("./key/dart_api_key.json",'r',encoding='utf-8') as f:
            key = json.load(f)   

        dart_key = key['dart_api_key']
        return dart_key
