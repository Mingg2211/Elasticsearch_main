from elasticsearch import Elasticsearch
# Khởi tạo Elasticsearch client
es = Elasticsearch(hosts="http://192.168.1.58:9200", verify_certs=False)

# Tạo index với tên là "news"
index_name = "mingg_v3"
# Có thể định nghĩa schema cho index làm khuân mẫu cho các Document(bản ghi)
# body = {field : value} 
es.indices.create(index=index_name)

