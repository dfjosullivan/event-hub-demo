from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es_client = Elasticsearch(["http://localhost:9200"])  # Replace with your Elasticsearch server address

# Elasticsearch index name
index_name = "example_index"

# Search query to retrieve all records (match_all)
query = {
    "query": {
        "match_all": {}
    }
}

# Perform the search
search_results = es_client.search(index=index_name, body=query, size=10000)  # Adjust 'size' as needed

# Process the search results
for hit in search_results["hits"]["hits"]:
    source_data = hit["_source"]
    print(source_data)