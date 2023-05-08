from elasticsearch import Elasticsearch
import requests
import datetime
from elasticsearch.helpers import bulk


        
def convert_to_json_standard(item):
    title = item['title']
    author = item['author']
    time = item['time']
    pub_date = item['pub_date'].isoformat()
    content = item['content']
    keywords = item['keywords']
    url = item['url']
    html = item['html']
    class_chude = item['class_chude']
    class_linhvuc = item['class_linhvuc']
    source_name = item['source_name']
    source_host_name = item['source_host_name']
    source_language = item['source_language']
    source_publishing_country = item['source_publishing_country']
    source_source_type = item['source_source_type']
    created_at = item['created_at']
    modified_at = item['modified_at']
    class_sacthai = item['class_sacthai']
    class_tinmau = item['class_tinmau']
    class_object = item['class_object']
    
    json_doc = {
        'data:title': title,
        'data:author': author,
        'data:time': time,
        'pub_date':{
            "$data":pub_date
            } ,
        'data:content': content,
        'keywords': keywords,
        'data:url': url,
        'data:html': html,
        'data:class_chude': class_chude,
        'data:class_linhvuc': class_linhvuc,
        'source_name': source_name,
        'source_host_name': source_host_name,
        'source_language': source_language,
        'source_publishing_country': source_publishing_country,
        'source_source_type': source_source_type,
        'created_at': created_at,
        'modified_at': modified_at,
        'data:class_sacthai': class_sacthai,
        'data:class_tinmau': class_tinmau,
        'data:class_object': class_object
    }
    
    return json_doc

class ElasticSearch:
    """Constructor function that initializes the ElasticSearch object with connection parameters."""
    def __init__(self, host, user, password, verify_certs):
        
        self.host = host
        self.user = user
        self.password = password
        self.verify_certs = verify_certs
        self.es = Elasticsearch(hosts=self.host, basic_auth=(self.user, self.password), verify_certs=self.verify_certs)

    """Creates an index in Elasticsearch with the specified schema."""
    def create_index(self, index_name, number_of_shards=1,GB=16):
        self.es.indices.create(index=index_name, body={
                                                        'settings' : {
                                                                'index' : {
                                                                    'index.translog.flush_threshold_size': '{GB}gb',
                                                                    'number_of_shards':number_of_shards
                                                                }
                                                        }})
        return 1
        
    def Insert_data_index(self, index_name, json_data):
        actions= []
        for item in json_data:
            item = item.dict()
            item = convert_to_json_standard(item)
            action = {
                '_index': index_name,
                '_op_type': 'index',  
                '_source': item 
            }
            actions.append(action)
        success, failed = bulk(self.es, actions)
        return "Successfully updated:", success ,'\n',"Failed to update:", failed
    
    def close_index(self, index_name):
        self.es.indices.close(index=index_name)
        return 1
        
    def open_index(self, index_name):
        self.es.indices.open(index=index_name)
        return 1
            
    def delete_index(self, index_name):
        self.es.indices.delete(index=index_name)
        return 1
    """Retrieves a document from Elasticsearch by its ID."""
    def get_by_id(self, index_name, doc_id):
        response = self.es.get(index=index_name, id=doc_id)
        document_data = response["_source"]
        return document_data

    """Searches for documents in an Elasticsearch index using a filter."""
    def search_using_filter(self, index_name, filter):
        searched = self.es.search(index=index_name, body=filter)
        result = []
        hits = searched['hits']['hits']                
        if hits:
            for hit in hits:
                result.append(hit)
        else:
            return -1
        
        return result

    """Searches for documents in an Elasticsearch index using a match query."""
    def search_match_params(self, index_name, search_params):
        query = {
            "query": {
                "multi_match": {
                    "query": search_params["query"],
                    "fields": list(search_params["fields"])
                }
            }
        }

        searched = self.es.search(index=index_name, body=query)

        result = []
        hits = searched['hits']['hits']
        if hits:
            for hit in hits:
                result.append(hit)
        else:
            return -1
        return result
    
    """Searches for documents in an Elasticsearch index using a must query."""
    def search_must_params(self, index_name, search_params):
        query = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        # Construct the match query for each field and value in search_params
        for field, value in search_params.items():
            query["query"]["bool"]["must"].append({
                "match": {
                    field: value
                }
            })

        searched = self.es.search(index=index_name, body=query)

        result = []
        hits = searched['hits']['hits']
        if hits:
            for hit in hits:
                result.append(hit)
        else:
            return -1

        return result

    """Updates a document in an Elasticsearch index."""
    def update_document(self, index_name, doc_id, update_data):
        body = {
            "doc": update_data
        }

        response = self.es.update(index=index_name, id=doc_id, body=body,doc_type='_doc')

        if response['_shards']['successful'] > 0:
            return 1
        else:
            return -1
            
    """Deletes a document from an Elasticsearch index."""    
    def delete_document(self, index_name, doc_id):
        response = self.es.delete(index=index_name, id=doc_id)

        if response['_shards']['successful'] > 0:
            return 1
        else:
            return -1

    """Retrieves information about an Elasticsearch index."""
    def log_index_info(self, index_name):
        response = self.es.cat.indices(index=index_name, format="json")
        return response[0]

    """"Retrieves information about Elasticsearch nodes."""
    def log_node_info(self):
        url = self.host + "/_cat/indices"
        response = requests.get(url)
        return response.text
    
    """This function takes in a string query and applies rules to build a query for Elasticsearch search."""
    def query_process(self, query:str):
        """
        Xây dựng luật để tạo truy vấn
        AND : +
        OR : |
        EXACT_MATCH = ""
        GROUP = ()
        NOT = -
        VD : "Nga" + "Việt Nam" - "Trung Quốc"  
        """
        rules = {
            "AND": "+",
            "OR": "|",
            "NOT": "-",
        }
        terms = query.split(' ')
        i = 0
        while i < len(terms):
            if terms[i] in rules.values():
                terms[i] = list(rules.keys())[list(rules.values()).index(terms[i])]
            i += 1
        query_string = " ".join(terms)

        return query_string
    
    """This function performs a search on an Elasticsearch index based on the provided query and optional parameters such as the fields to search in, date range, and number of results to return."""
    def search_main(self,index_name, query,k=None ,fields=None, gte=None, lte=None):
        _query_string = self.query_process(query)
        
        default_fields = ["data:title^200", "data:content^3"]
        if fields is None:
            _fields = default_fields
        else :
            _fields = default_fields.extend(fields)

        if gte is None:
            _gte = "1990/03/31 17:14:14"
        else:
            _gte = gte
        if lte is None:
            _lte = "2990/03/31 17:14:14"
        else:
            _lte = lte
        
        ### request template
        filter = {
                "query": {
                    "bool": {
                        "must": {
                            "query_string": {
                                "query": _query_string,
                                "fields": _fields
                            }
                        },
                        "filter": {
                            "range": {
                                "created_at": {
                                    "gte": _gte,
                                    "lte": _lte
                                }
                            }
                        }
                    }
                },
                "sort": [
                {
                    "created_at": {
                        "order": "desc"
                    }
                }
            ]
        }
        
        searched = self.es.search(index=index_name, body=filter)
        result = []
        hits = searched['hits']['hits']                
        if hits:
            for hit in hits:
                result.append(hit)
        else:
            print("No matching documents found.")
            return []
        
        if k==None:
            return result
        else:
            return result[:k]

    
    
if __name__ == "__main__":
    item = {'title': 'string', 'author': 'string', 'time': 'string', 'pub_date': datetime.datetime(2023, 4, 12, 9, 57, 36, 277000, tzinfo=datetime.timezone.utc),
            'content': 'string', 'keywords': ['string'], 'url': 'string', 'html': 'string', 'class_chude': ['string'],
            'class_linhvuc': ['string'], 'source_name': 'string', 'source_host_name': 'string', 'source_language': 'string', 
            'source_publishing_country': 'string', 'source_source_type': 'string', 'created_at': 'string', 'modified_at': 'string',
            'class_sacthai': 'string', 'class_tinmau': ['string'], 'class_object': ['string']}
    
    print(convert_to_json_standard(item))
    
    # es = ElasticSearch(host='192.168.1.58:9200', user='USER', password='PASS', verify_certs=False)
    # index_name='sonba_test_connections'
    # es.create_index(index_name, number_of_shards=1,GB=2)
    
        
    # es = ElasticSearch(host='0.0.0.0:9200', user='USER', password='PASS', verify_certs=False)
    # index_name='my_index'
    # # es.delete_index('new')
    # query = '("hà nội" +"nghìn năm văn vở")  | ("thăng long")'
    # results=es.search_main(index_name, query,k=10 ,fields=None, gte=None, lte=None)
    # for item in results:
    #     print(item["_source"]['data:title'])