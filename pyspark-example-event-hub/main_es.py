import json
import os
import uuid
import datetime

from pyspark import SQLContext
from pyspark.shell import sc
# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import dotenv
from pyArango.connection import Connection
import logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

dotenv.load_dotenv()

ARANGO_COLLECTION_NAME=os.getenv("ARANGO_COLLECTION_NAME_ES")
ARANGO_USERNAME=os.getenv("ARANGO_USERNAME")
ARANGO_PASSWORD=os.getenv("ARANGO_PASSWORD")
ARANGO_URL=os.getenv("ARANGO_URL")
ARANGO_DB=os.getenv("ARANGO_DB")

ES_NODE=os.getenv("ES_NODE")
ES_USER=os.getenv("ES_USER")
ES_PASSWORD=os.getenv("ES_PASSWORD")

# Write assertions to check if the environment variables are not empty
assert ARANGO_COLLECTION_NAME, "ARANGO_COLLECTION_NAME is not set or empty"
assert ARANGO_USERNAME, "ARANGO_USERNAME is not set or empty"
assert ARANGO_PASSWORD, "ARANGO_PASSWORD is not set or empty"
assert ARANGO_URL, "ARANGO_URL is not set or empty"
assert ARANGO_DB, "ARANGO_DB is not set or empty"
assert ES_NODE, "ES_NODE is not set or empty"
assert ES_USER, "ES_USER is not set or empty"
assert ES_PASSWORD, "ES_PASSWORD is not set or empty"
#
# .config("es.nodes.wan.only", "true")  remove
#     .config("es.nodes.wan.only", "true") \
#     .config("es.nodes.discovery", "true") \
#     .config("es.net.ssl", "false") \
#     .config("es.nodes.client.only", "false") \
sc.setLogLevel("warn")
spark_session = SparkSession.builder \
    .master("spark://localhost:7077") \
    .config("es.nodes", ES_NODE) \
    .config("es.port", "9200") \
    .config("es.net.http.auth.user", ES_USER) \
    .config("es.net.http.auth.pass", ES_PASSWORD) \
    .config("es.index.auto.create", "true") \
    .appName("Listen for record from elastic search") \
    .getOrCreate()


# If all assertions pass, print a success message
print("All environment variables are properly set.")


# Define the Elasticsearch index and query
es_index = "your-elasticsearch-index"
es_query = '{ "query": { "match_all": {} }}'


#streaming_df = spark_session.readStream.format("org.elasticsearch.spark.sql") \
#    .option("es.resource", es_index) \
#    .option("es.query", es_query) \
#    .load()





arango_connection = Connection(arangoURL=ARANGO_URL, username=ARANGO_USERNAME, password=ARANGO_PASSWORD)
database_name = ARANGO_DB

# Check if the database exists
if not arango_connection.hasDatabase(database_name):
    # Create the database if it does not exist
    database = arango_connection.createDatabase(name=database_name)
    print(f"Database '{database_name}' created.")
else:
    database = arango_connection[database_name]
    print(f"Database '{database_name}' already exists.")


# Specify the collection name
collection_name = ARANGO_COLLECTION_NAME

# Check if the collection exists
if not database.hasCollection(collection_name):
    # Create the collection if it does not exist
    collection = database.createCollection(className="Collection", name=collection_name)
    print(f"Collection '{collection_name}' created.")
else:
    collection = database[collection_name]
    print(f"Collection '{collection_name}' already exists.")

def process_batch(df, epoch_id):
    logging.warning(f"Processing micro-batch {epoch_id}")
    logging.warning(f"Processing micro-batch of size {str(df.count())}")
    decoded_fields = []
    for row in df.collect():
        logging.warning(f"Data: {row.decoded_field}")
        document = json.loads(row.decoded_field)
        document["_key"] = str(uuid.uuid4())
        document["processor"] = "Processed By Pyspark"
        current_timestamp = datetime.datetime.now()
        document["date_processed"] = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        logging.warning(f"Data Uploaded: {document}")
        collection.createDocument(document).save()
        decoded_fields.append(row.decoded_field)

    log_path = "log_output.json"

    with open(log_path, "w") as log_file:
        json.dump(decoded_fields , log_file, indent=2)

    logging.warning(f"Processed micro-batch {epoch_id}")

#db_query = streaming_df.writeStream \
#    .foreachBatch(process_batch)  \
#    .format("console") \
#    .start() \
#    .awaitTermination()

df = spark_session.read.format("org.elasticsearch.spark.sql") \
    .option("es.resource", es_index) \
    .option("es.query", es_query) \
    .load()


df.show()