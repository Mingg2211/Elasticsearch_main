from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

# define a list of updates to apply
updates = [
    {
        "_index": "news_",
        "_type": "_doc",
        "_id": "1",
        "_source": {"title": "New Title 1"}
    },
    {
        "_index": "news_",
        "_type": "_doc",
        "_id": "2",
        "_source": {"title": "New Title 2"}
    }
]

# use the bulk API to update the documents
success, failed = bulk(es, updates)

# print the results
print("Successfully updated:", success)
print("Failed to update:", failed)