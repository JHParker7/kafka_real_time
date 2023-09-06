import sqlalchemy as sql
import pandas as pd
from kafka import KafkaConsumer
import json
from io import StringIO

conn=sql.create_engine("postgresql+pg8000://postgres:postgres@localhost:5438")

consumer = KafkaConsumer(
    'data',
    bootstrap_servers=['localhost:9093'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(consumer.poll(100))
x=0
for message in consumer:
    print("message received")
    data=message.value
    data=pd.read_json(StringIO(data))

  #  print(" ")
   # print(data.iloc[0].to_list())

    #add proccessing here
    data=data.fillna("This is was a Null")
    print("message proccesed")
   # print(data.iloc[0].to_list())
    data.to_sql("test",con=conn, if_exists="append",index=False)
    print("message added to DB")