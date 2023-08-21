import json
import os
import uuid
import datetime
from pyspark.shell import sc
# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import dotenv
from pyArango.connection import Connection
import logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

dotenv.load_dotenv()

sc.setLogLevel("warn")
spark_session = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("Listen for new files added to azure blob storage") \
    .getOrCreate()

# Define the schema for the incoming JSON data
#x = spark.conf.get("spark.loadedJars")

schema = StructType().add("message", StringType())

BLOB_STORAGE_CONNECTION_STRING= os.getenv("BLOB_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME=os.getenv("BLOB_CONTAINER_NAME")
EVENT_HUB_CONNECTION_STRING=os.getenv("EVENT_HUB_CONNECTION_STRING_WAD")
EVENT_HUB_NAME=os.getenv("EVENT_HUB_NAME_WAD")
ARANGO_COLLECTION_NAME=os.getenv("ARANGO_COLLECTION_NAME_WAD")
ARANGO_USERNAME=os.getenv("ARANGO_USERNAME")
ARANGO_PASSWORD=os.getenv("ARANGO_PASSWORD")
ARANGO_URL=os.getenv("ARANGO_URL")
ARANGO_DB=os.getenv("ARANGO_DB")

# Write assertions to check if the environment variables are not empty
assert BLOB_STORAGE_CONNECTION_STRING, "BLOB_STORAGE_CONNECTION_STRING is not set or empty"
assert BLOB_CONTAINER_NAME, "BLOB_CONTAINER_NAME is not set or empty"
assert EVENT_HUB_CONNECTION_STRING, "EVENT_HUB_CONNECTION_STRING is not set or empty"
assert EVENT_HUB_NAME, "EVENT_HUB_NAME is not set or empty"
assert ARANGO_COLLECTION_NAME, "ARANGO_COLLECTION_NAME is not set or empty"
assert ARANGO_USERNAME, "ARANGO_USERNAME is not set or empty"
assert ARANGO_PASSWORD, "ARANGO_PASSWORD is not set or empty"
assert ARANGO_URL, "ARANGO_URL is not set or empty"
assert ARANGO_DB, "ARANGO_DB is not set or empty"

# If all assertions pass, print a success message
print("All environment variables are properly set.")

starting_offsets = {
    "0": "-1", # latest offset
}

cn_string = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EVENT_HUB_CONNECTION_STRING)

# Configure the connection to the Event Hub
eventhubs_conf = {
    "eventhubs.connectionString": cn_string,
    "eventhubs.eventHubName": EVENT_HUB_NAME,
    "eventhubs.consumerGroup": "$Default",
    "startingPosition": "earliest"
}

# Read events from Azure Event Hub
stream_data = spark_session\
    .readStream \
    .format("eventhubs") \
    .options(**eventhubs_conf) \
    .load()

# Parse JSON data
#parsed_events = stream_data.select(from_json(stream_data.body.cast("string"), schema).alias("data"))
selected_field_df = stream_data.select(col("body").cast("string").alias("decoded_field"))
# Start processing



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

db_query = selected_field_df.writeStream \
    .foreachBatch(process_batch)  \
    .outputMode("append") \
    .start() \
    .awaitTermination()

