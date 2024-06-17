import sqlalchemy as sql
import pandas as pd
import pickle


engine = sql.create_engine('mysql+mysqldb://tgbot:tgbot%40123@51.254.115.58/tgbot?charset=utf8mb4')

with open('backup_data.pickle', 'rb') as f:
    data = pickle.load(f)
    
    
data.to_sql(con=engine, schema="tgbot", name="DATA", if_exists="append", index=False, chunksize=1000, method='multi')
