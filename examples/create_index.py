import requests
import time

# Elasticsearch index name
index_name = "example_index"

# Initial PUT request to create the index
put_url = f"http://localhost:9200/{index_name}"
put_response = requests.put(put_url)
print("Index creation response:", put_response.text)

# Loop to