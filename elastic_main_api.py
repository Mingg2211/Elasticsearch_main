from fastapi import FastAPI
from elastic_main import My_ElasticSearch
import uvicorn
from typing import List, Optional, Union
from pydantic import BaseModel,Field
from datetime import datetime, timezone
from starlette.middleware.cors import CORSMiddleware


def datetime_format(date_string):
    # VD date_string : 2023-03-24 00:00:00
    # my_format = "%Y-%m-%d %H:%M:%S"
    local_dt = datetime.fromisoformat(date_string)

    # Convert local datetime object to UTC datetime object
    utc_dt = local_dt.astimezone(timezone.utc)

    # Format UTC datetime object to ISO 8601 format with UTC timezone
    elastic_formatted_time = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return elastic_formatted_time




app = FastAPI()
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])

my_es = My_ElasticSearch(host=['http://192.168.1.58:9200'], user='USER', password='PASS', verify_certs=False)

# date_format = "%Y-%m-%d"
class Document(BaseModel):
    title : str = Field(...)
    author : str = Field(...)
    time : str = Field(...)
    pub_date : str = Field(...)
    content : str = Field(...)
    keywords : List[str] = Field(...)
    url : str = Field(...)
    html : str = Field(...)
    class_chude : List[str] = Field(...)
    class_linhvuc : List[str] = Field(...)
    source_name : str = Field(...)
    source_host_name : str = Field(...)
    source_language : str = Field(...)
    source_publishing_country : str = Field(...)
    source_source_type : str = Field(...)
    created_at : str = Field(...)
    modified_at : str = Field(...)
    class_sacthai : str = Field(...)
    class_tinmau : List[str] = Field(...)
    class_object : List[str] = Field(...)

class Index(BaseModel):
    index_name : str = Field(...)

class Index_Docid(BaseModel):
    index_name : str = Field(...)
    doc_id : str = Field(...)

class Update(BaseModel):
    index_name : str = Field(...)
    doc_id : str = Field(...)
    update_data : Document = Field(...)

class List_Document(BaseModel):
    list_doc :List[Document] = Field(...)

class Query(BaseModel):
    index_name : str = Field(...)
    question : str = Field(...)
    lang : List[Union[str, None]] = None
    sentiment : Union[str, None] = None
    gte : Union[str, None] = None
    lte : Union[str, None] = None
    k : int = Field(...)
# @app.post("/insert_one_document")
# async def insert_one_document_(document:dict):
#     print(document)
#     insert_log = my_es.insert_document(index_name, document)
#     return {"output": insert_log}

@app.get("/")
async def hello():
    return {"Welcome to": " Elasticsearch"}

@app.get("/log_cluster_health")
async def log_cluster_health():
    return {"Cluster health": my_es.log_cluster_health()}
@app.get("/log_nodes_info")
async def log_nodes_info():
    return {"Nodes info": my_es.log_nodes_info()}
@app.get("/show_all_indexes_in_cluster")
async def show_all_indexes_in_cluster():
    return {"All indexes": my_es.show_all_indexes_in_cluster()}

@app.post("/log_index_health")
async def log_index_health(index: Index):
    return {"Index health": my_es.log_index_health(index_name=index.index_name)}
@app.post("/check_info_index")
async def check_info_index(index: Index):
    return {"Index info": my_es.check_info_index(index_name=index.index_name)}
@app.post("/index_head")
async def index_head(index: Index):
    return {"Index head": my_es.index_head(index_name=index.index_name)}
@app.post("/create_new_index")
async def create_new_index(index: Index):
    return {"Log": my_es.create_new_index(index_name=index.index_name)}
@app.post("/delete_index")
async def delete_index(index: Index):
    return {"Log": my_es.delete_index(index_name=index.index_name)}

@app.post("/insert_one_document")
async def insert_one_document(index_name:str,document:Document):
    json_doc = dict(document)
    json_doc['pub_date'] = datetime_format(json_doc['pub_date'])
    json_doc['created_at'] = datetime_format(json_doc['created_at'])
    json_doc['modified_at'] = datetime_format(json_doc['modified_at'])
    print(json_doc)
    insert_log = my_es.insert_document(index_name, json_doc)
    return {"Log": insert_log}

@app.post("/insert_many_document")
async def insert_many_document(index_name:str,_list_document: List_Document):
    list_document = _list_document["list_doc"]
    list_json_doc = [dict(doc) for doc in list_document]
    for json_doc in list_json_doc:
        json_doc['pub_date'] = datetime_format(json_doc['pub_date'])
        json_doc['created_at'] = datetime_format(json_doc['created_at'])
        json_doc['modified_at'] = datetime_format(json_doc['modified_at'])
    print(list_json_doc)
    insert_log = my_es.insert_many_document(index_name, list_json_doc)
    return {"Log": insert_log}
@app.post("/check_number_document")
async def check_number_document(index: Index):
    return {"Log": my_es.check_number_document(index_name=index.index_name)}

@app.post("/get_document_by_id")
async def get_document_by_id(index_docid: Index_Docid):
    return {"Log": my_es.get_document_by_id(index_name=index_docid.index_name, doc_id=index_docid.doc_id)}

@app.post("/update_document")
async def update_document(update: Update):
    return {"Log": my_es.update_document(index_name=update.index_name, doc_id=update.doc_id, update_data=update.update_data)}

@app.post("/delete_document")
async def delete_document(index_docid: Index_Docid):
    return {"Log": my_es.delete_document(index_name=index_docid.index_name, doc_id=index_docid.doc_id)}

@app.post("/searching")
async def searching(query:Query):
    gte = datetime_format(query.gte)
    lte = datetime_format(query.lte)
    result = my_es.search_main(index_name=query.index_name,query=query.question,k=query.k,lang=query.lang, sentiment=query.sentiment,gte=gte,lte=lte)
    return {"Result": result}

if __name__ == '__main__':
    uvicorn.run("elastic_main_api:app", host="localhost", port=9202, reload=True)
