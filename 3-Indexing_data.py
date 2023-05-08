from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200", basic_auth=('USER', 'PASS'), verify_certs=False)

index_name = "news"

doc = {"title": "Sách lập trình Python", "author": "John Smith"}

res = es.index(index=index_name, doc_type="_doc", body=doc)

print(res['result'])