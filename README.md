docker-compose up -d

docker-compose down

Must use python 3.9

sudo apt install python3.10

Spark UI
http://localhost:9090/

../venv/bin/spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22 --packages com.microsoft.azure:azure-eventhubs:3.2.2:3.3.0 --master spark://172.18.0.2:7077 main.py

Run the job with:

../venv/bin/spark-submit --master spark://localhost:7077 main.py

Copy to pyspark/jars


Reference
https://medium.com/@mehmood9501/using-apache-spark-docker-containers-to-run-pyspark-programs-using-spark-submit-afd6da480e0f