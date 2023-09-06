from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np
import sqlalchemy as sql
import random
import string

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for x in range(100):
    df=pd.DataFrame([[np.random.randint(0,1000),np.random.randint(0,1000),np.random.randint(0,1000),np.random.randint(0,1000),np.random.randint(0,1000),np.random.randint(0,1000),np.random.randint(0,1000), random.choice(string.ascii_letters), random.choice(string.ascii_letters), random.choice(string.ascii_letters), random.choice(string.ascii_letters)+random.choice(string.ascii_letters)+ random.choice(string.ascii_letters),np.nan]])
    print(df)
    producer.send('data',df.to_json())

conn=sql.create_engine("postgresql+pg8000://postgres:postgres@localhost:5438")
print("""




""")
print(pd.read_sql_table("test",con=conn))