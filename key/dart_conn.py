import json

class dartKey:
    def dart_key():
        with open("./key/dart_api_key.json",'r',encoding='utf-8') as f:
            key = json.load(f)   

        dart_key = key['dart_api_key']
        return dart_key
