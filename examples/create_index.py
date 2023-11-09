import requests
import time

# Elasticsearch index name
index_name = "example_index3"


mappings = {
  "mappings": {
    "properties": {
      "decoded_field": {
      "type": "nested",
      "properties": {
      "records": {
        "type": "nested",
        "properties": {
          "time": { "type": "date" },
          "category": { "type": "keyword" },
          "level": { "type": "keyword" },
          "properties": {
            "type": "object",
            "properties": {
              "DeploymentId": { "type": "keyword" },
              "Role": { "type": "keyword" },
              "RoleInstance": { "type": "keyword" },
              "ProviderGuid": { "type": "keyword" },
              "ProviderName": { "type": "keyword" },
              "EventId": { "type": "integer" },
              "Level": { "type": "integer" },
              "Pid": { "type": "integer" },
              "Tid": { "type": "integer" },
              "Opcode": { "type": "integer" },
              "Task": { "type": "integer" },
              "Channel": { "type": "keyword" },
              "Description": { "type": "text" },
              "RawXml": { "type": "text" }
            }
          }
        }}
      }
      }
    }
  }
}


# Initial PUT request to create the index
put_url = f"http://localhost:9200/{index_name}"
put_response = requests.put(put_url, json=mappings)
print("Index creation response:", put_response.text)

# Loop to