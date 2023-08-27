import requests
import time

# Elasticsearch index name
index_name = "example_index"

num_iterations = 10  # Change this to the desired number of iterations
for num in range(1, num_iterations + 1):
    payload = {
        "test": f"test{num}",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    post_url = f"http://localhost:9200/{index_name}/_doc/"
    post_response = requests.post(post_url, json=payload)

    print(f"POST request {num} response:", post_response.text)

    time.sleep(1)  # Add a delay between requests to avoid overwhelming the server

print("Done!")