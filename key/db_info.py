import json

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
        # conn = pymysql.connect(
        #     user=user,
        #     password=password,
        #     host = host,
        #     port = int(port),
        #     database=database,
        #     # charset=charset                     
        #     )
        # curs = conn.cursor()
        # print(curs)
        


    