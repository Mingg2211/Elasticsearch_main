import pymongo
from bson.json_util import dumps
import json
from elasticsearch import Elasticsearch
from datetime import datetime ,timezone

myclient = pymongo.MongoClient("mongodb://vosint:vosint_2022@192.168.1.100:27017/?authMechanism=DEFAULT")

db = myclient.vosint_db

collection = db.News

cursor = collection.find({})
list_cur = list(cursor)


# for cur in list_cur:
#     del cur['_id']
#     if 'pub_date' in cur.keys():
#         cur['pub_date'] = "2023-03-28T00:00:00Z"
#     else:
#         cur.update({"pub_date":"2023-03-28T00:00:00Z"})
#     # Input string
#     date_str = cur['created_at']
#     # Convert string to datetime object
#     dt_obj = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")

#     # Format datetime object to ISO 8601 format with UTC timezone
#     formatted_time = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

#     # Print the formatted time
#     cur['created_at'] = formatted_time


#     # Input string
#     date_str = cur['modified_at']
#     # Convert string to datetime object
#     dt_obj = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")

#     # Format datetime object to ISO 8601 format with UTC timezone
#     formatted_time = dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    
#     # Print the formatted time
#     cur['modified_at'] = formatted_time
    
# print(list_cur[0])
  


json_data = dumps(list_cur, indent = 2,ensure_ascii=False) 
   
# Writing data to file data.json
with open('collection_test.json', 'w', encoding='utf-8') as file:
    file.write(json_data)



#  Khởi tạo kết nối với Elasticsearch
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Mảng đối tượng JSON
# data =json.loads(dumps(cursor))



# for d in data:
    # del d['_id']
    
# # Gửi dữ liệu vào Elasticsearch
# for doc in data:
    # es.index(index='mingg', doc_type='_doc', body=doc)
# document = data[0]
# print(document)
# with open('keys.txt', 'w', encoding='utf-8') as f:
#     for key in document.keys():
#         f.write('"'+str(key)+'" : "",' + '\n')