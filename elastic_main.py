from elasticsearch import Elasticsearch
import requests
import datetime
from elasticsearch.helpers import bulk
import elasticsearch
import json
# import warnings
# from elasticsearch.exceptions import ElasticsearchWarning
# warnings.simplefilter('ignore', ElasticsearchWarning)

class My_ElasticSearch:
    def __init__(self, host, user, password, verify_certs):
        """Constructor function that initializes the ElasticSearch object with connection parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.verify_certs = verify_certs
        self.es = Elasticsearch(hosts=self.host, verify_certs=self.verify_certs)
        
    def log_cluster_health(self):
        """
        Kiểm tra sức khỏe cụm
        Returns:
            Log
        """   
        return self.es.cluster.health()
    
    def log_nodes_info(self):
        """
        In ra thông tin các nodes
        Returns:
            Log
        """  
        return self.es.nodes.info()
    
    def show_all_indexes_in_cluster(self):
        """
        In ra tất cả các index trong cụm
        Returns:
            Log
        """
        return self.es.indices.get_alias(index="*")

    def log_index_health(self, index_name:str):
        """
        Kiểm tra sức khỏe index
        Args:
            index_name (str): tên index
        Returns:
            Log
        """ 
        response = self.es.cat.indices(index=index_name, format="json")
        return response[0]
    
    def check_info_index(self, index_name:str):
        """
        Kiểm tra thông tin index
        Args:
            index_name (str): tên index
        Returns:
            Log
        """
        if self.es.indices.exists(index=index_name):
            index_info = self.es.indices.get(index=index_name)
            return 'Thông tin về Index {index_name}: {index_info}'.format(index_name = index_name, index_info = index_info)
        else :
            return 'Index không tồn tại'
    
    def index_head(self, index_name:str):
        """
        In ra 5 document trong index
        Args:
            index_name (str): tên index
        Returns:
            5 documents
        """  
        if self.es.indices.exists(index=index_name):
            res = self.es.search(index=index_name, body={"query": {"match_all": {}}, "size": 5})
            # In các document ra màn hình
            top_5 = []
            for hit in res['hits']['hits']:
                top_5.append(hit['_source'])
            return top_5
        else :
            return 'Index không tồn tại'
        
    def create_new_index(self, index_name:str):
        """
        Tạo index mới
        Args:
            index_name (str): tên index
        Returns:
            Log
        """ 
        
        # date format UTC
        body_schema = {
            'mappings': {
                'properties': {
                    'name': 
                        {
                            "type": "text"
                                },
                    "title" : 
                        {
                            "type": "text"
                                },
                    "author" : 
                        {
                            "type": "text"
                                },
                    "time" : 
                        {
                            "type": "text",
                            },
                    "pub_date" : 
                        {
                            "type": "date",
                            },
                    "content" : 
                        {
                            "type": "text"
                            },
                    "url" : 
                        {
                            "type": "text"
                            },
                    "html" : 
                        {
                            "type": "text"
                            },
                    "class_chude" : 
                        {
                            "type": "text"
                            },
                    "class_linhvuc" : 
                        {
                            "type": "text"
                            },
                    "source_name" : 
                        {
                            "type" : "text"
                            },
                    "source_host_name" : 
                        {
                            "type" : "text"
                            },
                    "source_language" : 
                        {
                            "type" : "text"
                            },
                    "source_publishing_country" : 
                        {
                            "type" : "text"
                            },
                    "source_source_type" : 
                        {
                            "type" : "text"
                            },
                    "created_at" : 
                        {
                            "type" : "date"
                            },
                    "modified_at" : 
                        {
                            "type" : "date"
                            },
                    "class_sacthai" : 
                        {
                            "type" : "text"
                            },
                    "class_tinmau" : 
                        {
                            "type" : "text"
                            },
                    "class_object" : 
                        {
                            "type" : "text"
                            },
                        }
                    }
            }
        if self.es.indices.exists(index=index_name):
            return 'Index đã tồn tại'
        else :
            self.es.indices.create(index=index_name, body=body_schema)
            return 'Index được khởi tạo thành công'
        
    def delete_index(self, index_name:str):
        """
        Xóa một index
        Args:
            index_name (str): tên index
        Returns:
            Log
        """
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
            return 'Index đã xóa'
        else :
            return 'Index không tồn tại'
        
    

    def insert_document(self, index_name:str, document:dict):
        """
        Thêm document vào index
        Args:
            index_name (str): tên index
            document (doc): document cần thêm
        Returns:
            Log
        
            Ví dụ document: doc_ = {
                            "title": "Thực hư thông tin Bình Tinh được cố nghệ sĩ Vũ Linh \"để lại hết tài sản\"",
                            "author": "Minggz",
                            "time": "Thứ ba, 28/03/2023 - 14:56",
                            "pub_date": "2023-03-28T00:00:00Z",
                            "content": "Tối 27/3, nghệ sĩ Bình Tinh - con gái nuôi của cố nghệ sĩ Vũ Linh - gây chú ý khi đăng tải bức ảnh",
                            "keywords": [
                                "Bình_Tinh",
                                "Vũ_Linh",
                                "nghệ_sĩ",
                                "nuôi",
                                "NSƯT",
                                "diễn",
                                "gái",
                                "tang",
                                "qua_đời",
                                "hậu_sự",
                                "lo",
                                "lễ"
                            ],
                            "url": "https://dantri.com.vn/van-hoa/thuc-hu-thong-tin-binh-tinh-duoc-co-nghe-si-vu-linh-de-lai-het-tai-san-20230328063244094.htm",
                            "html": "<p>Tối 27/3, nghệ sĩ Bình Tinh - con gái nuôi của cố nghệ sĩ Vũ Linh - gây chú ý khi đăng tải bức ảnh </p>",
                            "class_chude": [],
                            "class_linhvuc": [],
                            "source_name": "dantri1",
                            "source_host_name": "dantri.com.vn",
                            "source_language": "vi",
                            "source_publishing_country": "Việt Nam",
                            "source_source_type": "Báo điện tử",
                            "created_at": "2023-03-28T00:00:00Z",
                            "modified_at": "2023-03-28T00:00:00Z",
                            "class_sacthai": "0",
                            "class_tinmau": [],
                            "class_object": []
                        }
        """
        try:
            self.es.index(index=index_name,doc_type='_doc', body=document)
            return 'Document đã được thêm vào Index'
        except elasticsearch.exceptions.RequestError as e:
            return (f"Error: {e} \n Có thể lỗi là do bạn truyền Document không đúng định dạng được định nghĩa trong Index !")
    
    def insert_many_document(self, index_name:str, list_of_document: list):
        """
        Thêm nhiều document vào index
        Args:
            index_name (str): tên index
            list_of_document (list[doc]): danh sách document cần thêm
        Returns:
            Log
            Ví dụ một document: doc_ = {
                            "title": "Thực hư thông tin Bình Tinh được cố nghệ sĩ Vũ Linh \"để lại hết tài sản\"",
                            "author": "Minggz",
                            "time": "Thứ ba, 28/03/2023 - 14:56",
                            "pub_date": "2023-03-28T00:00:00Z",
                            "content": "Tối 27/3, nghệ sĩ Bình Tinh - con gái nuôi của cố nghệ sĩ Vũ Linh - gây chú ý khi đăng tải bức ảnh",
                            "keywords": [
                                "Bình_Tinh",
                                "Vũ_Linh",
                                "nghệ_sĩ",
                                "nuôi",
                                "NSƯT",
                                "diễn",
                                "gái",
                                "tang",
                                "qua_đời",
                                "hậu_sự",
                                "lo",
                                "lễ"
                            ],
                            "url": "https://dantri.com.vn/van-hoa/thuc-hu-thong-tin-binh-tinh-duoc-co-nghe-si-vu-linh-de-lai-het-tai-san-20230328063244094.htm",
                            "html": "<p>Tối 27/3, nghệ sĩ Bình Tinh - con gái nuôi của cố nghệ sĩ Vũ Linh - gây chú ý khi đăng tải bức ảnh </p>",
                            "class_chude": [],
                            "class_linhvuc": [],
                            "source_name": "dantri1",
                            "source_host_name": "dantri.com.vn",
                            "source_language": "vi",
                            "source_publishing_country": "Việt Nam",
                            "source_source_type": "Báo điện tử",
                            "created_at": "2023-03-28T00:00:00Z",
                            "modified_at": "2023-03-28T00:00:00Z",
                            "class_sacthai": "0",
                            "class_tinmau": [],
                            "class_object": []
                        }
        """
        bulk_action = [
            {
                '_index' : index_name,
                '_doc' : doc
            }
            for doc in list_of_document
        ]
        try :
            bulk(self.es, bulk_action)
            return 'Document đã được thêm vào Index'
        except elasticsearch.exceptions.RequestError as e:
            return (f"Error: {e} \n Có thể lỗi là do bạn truyền Document không đúng định dạng được định nghĩa trong Index!")
    
    def check_number_document(self, index_name:str):
        """
        Kiểm tra số lượng document trong index
        Args:
            index_name (str): tên index
        Returns:
            Log : số lượng document
        """ 
        if self.es.indices.exists(index=index_name):
            count = self.es.count(index=index_name)['count']
            return 'Số lượng Document có trong Index {index_name}: {count}'.format(index_name = index_name, count = count)
        else :
            return 'Index không tồn tại'
    
    
    
    def get_document_by_id(self, index_name:str, doc_id:str):
        """
        Lấy ra document bằng id
        Args:
            index_name (str): tên index
            doc_id (str): id của document
        Returns:
            document
        """
        response = self.es.get(index=index_name, id=doc_id)
        document_data = response["_source"]
        return document_data
    
    def update_document(self, index_name:str, doc_id:str, update_data: dict):
        """
        Cập nhật document bằng id
        Args:
            index_name (str): tên index
            doc_id (str): id của document
            update_data (dict): trường và dữ liệu cần thay đổi
        Returns:
            _type_: 1 ; -1
        """
        body = {
            "doc": update_data
        }

        response = self.es.update(index=index_name, id=doc_id, body=body,doc_type='_doc')

        if response['_shards']['successful'] > 0:
            return 1
        else:
            return -1
    
    
    def delete_document(self, index_name:str, doc_id:str):
        """
        Xóa document bằng id
        Args:
            index_name (str): tên index
            doc_id (str): id của document
        Returns:
            _type_: 1 ; -1
        """
        response = self.es.delete(index=index_name, id=doc_id,doc_type='_doc')

        if response['_shards']['successful'] > 0:
            return 1
        else:
            return -1
    ######################## SEARCHING ########################
    
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
    
    
    
    def search_main(self,index_name, query,k=None ,sentiment=None,lang=None, gte=None, lte=None):
        """ Tìm kiếm document theo query
        Args:
            index_name (str): Tên index
            query (str): "Nga" + "Việt Nam" - "Trung Quốc"  
            k (int, optional): Số lượng tối đa đầu ra. Defaults to None.
            fields (list[str], optional): Các trường tìm kiếm. Defaults to None.
            gte (str, optional), lte (str, optional): Khoảng thời gian tìm kiếm
        Returns:
            Log : list[doc] | None
        """
        _query_string = self.query_process(query)
        print(_query_string)
        _fields = ["title^2", "content"]
        if gte is None:
            _gte = "1990-03-28T00:00:00Z"
        else:
            _gte = gte
        if lte is None:
            _lte = "2090-03-28T00:00:00Z"
        else:
            _lte = lte
        if sentiment is None:
            _sentiment = "*"
        else:
            _sentiment = sentiment
        if lang is None:
            print("Lang is none")
            _lang = "*"
        else:
            _lang = lang
        ### request template
        print(type(_lang))
        _lang_query = " OR ".join(_lang)
        print(_sentiment)
        print(_lang_query)
        simple_filter = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {
                                    "query": _query_string,
                                    "fields": _fields
                                }
                            },
                            {
                                "query_string":{
                                    "query": _lang_query,
                                    "default_field": "source_language"
                                }
                            },
                            {
                                "query_string":{
                                    "query": _sentiment,
                                    "default_field": "class_sacthai"
                                }
                            }
                        ],
                        "filter": {
                            "range": {
                                "pub_date": {
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
        
        # print(filter)
        
        
        searched = self.es.search(index=index_name, body=simple_filter)
        result = []
        hits = searched['hits']['hits']            
        if hits:
            for hit in hits:
                result.append(hit)
        else:
            print("Không tìm thấy Document nào")
            return []
        
        if k==None:
            return ("Tìm thấy {k} Document match với query của bạn \n\n Result : {R}".format(k=len(result), R=result))
        else:
            return result[:k]
            
