from elasticsearch import Elasticsearch
# Kết nối đến Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200", basic_auth=('USER', 'PASS'), verify_certs=False)

# Specify the index name
index_name = "my_index"


# Send a request to Elasticsearch to get the mapping information of the index
response = es.indices.get_mapping(index=index_name)

# Extract the mapping information from the response
mapping = response[index_name]["mappings"]

# Access the fields and their data types in the mapping
fields = mapping.keys()

# Print the fields in the mapping
print(f"Fields in Index '{index_name}':")
for field in fields:
    print(field)
"""

# Define the index name and search query
index_name = 'my_index'  # Replace with your desired index name
search_query = {
    "query": {
        "match": {
            "data:title": "python hau"  # Replace with the field name and search term
        }
    }
}

# Perform the search in Elasticsearch
result = es.search(index=index_name, body=search_query)

# Extract the search results
hits = result['hits']['hits']

# Process the search results
if hits:
    print(f"Total hits: {result['hits']['total']['value']}")
    print("Search results:")
    for hit in hits:
        print(f"Document ID: {hit['_id']}")
        print(f"Document source: {hit['_source']}")
                
        # Count the number of fields in the document
        print(hit['_source'].keys())
        print("1209834120983201983012938")
        break
else:
    print("No matching documents found.")
    

# Send a request to Elasticsearch to get information about the index
response = es.cat.indices(index=index_name, format="json")

# Extract the index information from the response
index_info = response[0]
print(index_info)

"""