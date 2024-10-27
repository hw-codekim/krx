import json
import os

# class connectDB():
#     @staticmethod
#     def db_conn():
#         user = os.getenv('DB_USER')
#         password = os.getenv('DB_PASSWORD')
#         host = os.getenv('DB_HOST')
#         port = os.getenv('DB_PORT')
#         database = os.getenv('DB_NAME')
#         return user, password, host, port, database
    
    
class connectDB():
    def db_conn():
        with open("./key/db_con.json",'r',encoding='utf-8') as f:
            data = json.load(f)    
            user = data['user']
            password = data['password']
            host = data['host']
            port = data['port']
            database = data['database']
            # charset = data['charset']
        return user,password,host,port,database
        


    