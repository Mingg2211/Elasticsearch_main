from fastapi import FastAPI
from elastic_search_tool import ElasticSearch
import uvicorn
from typing import List, Optional
from pydantic import BaseModel,Field
from datetime import datetime
from starlette.middleware.cors import CORSMiddleware


app = FastAPI()
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])

es = ElasticSearch(host='http://localhost:9200', user='USER', password='PASS', verify_certs=False)
index_name='my_index'

class Item(BaseModel):
    title: str = Field(alias='data:title')
    author: str = Field(alias='data:author')
    time: str = Field(alias='data:time')
    pub_date: datetime = Field(alias='pub_date.$date')
    content: str = Field(alias='data:content')
    keywords: List[str] = Field()
    url: str = Field(alias='data:url')
    html: str = Field(alias='data:html')
    class_chude: List[str] = Field(alias='data:class_chude')
    class_linhvuc: List[str] = Field(alias='data:class_linhvuc')
    source_name: str = Field()
    source_host_name: str = Field()
    source_language: str = Field()
    source_publishing_country: str = Field()
    source_source_type: str = Field()
    created_at: str = Field()
    modified_at: str = Field()
    class_sacthai: str = Field(alias='data:class_sacthai')
    class_tinmau: List[str] = Field(alias='data:class_tinmau')
    class_object: List[str] = Field(alias='data:class_object')


@app.post("/insert_data")
async def insert_data(json_data: List[Item]):
    insert_log = es.Insert_data_index(index_name, json_data)
    return {"output": insert_log}

@app.post("/searching")
async def searching(index_name:str, query:str,k:int=None ,fields:List[str]=None, gte:datetime=None, lte:datetime=None):
    result = es.search_main(index_name, query,k=k,fields=fields,gte=gte,lte=lte)
    return {"output": result}

@app.post("/log_node_info")
async def log_node_info():
    result = es.log_node_info()
    return {"output": result}

@app.post("/log_index_info")
async def log_index_info(index_name:str):
    result = es.log_index_info(index_name)
    return {"output": result}

@app.post("/delete_document")
async def delete_document(index_name:str, doc_id:str):
    result = es.delete_document(index_name, doc_id)
    return {"output": result}

@app.post("/update_document")
async def update_document(index_name:str, doc_id:str,update_data:dict):
    result = es.update_document(index_name, doc_id,update_data)
    return {"output": result}

@app.post("/get_by_id")
async def get_by_id(index_name:str, doc_id:str):
    result = es.get_by_id(index_name, doc_id)
    return {"output": result}



if __name__ == '__main__':
    uvicorn.run("elastic_api:app", host="localhost", port=9201, reload=True)