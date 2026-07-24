producer ---> Model Building pipeline -> 
		 ---> Analytics Pipeline
		 ---> Prediction Pipeline --> model_prediction consumer -> Show to user
		 ---> Backend Process Triggers(yet to figure out)


#STEP:1
docker compose -f docker_compose.yaml up -d

#STEP:2
pip install kafka-python
python ./src/kafkaScripts/topicCreator.py

#Step:3
python ./src/kafkaScripts/launch_producer.py --n-users 1000 --n-products 200 --batch-size 500 --interval-seconds 5 --kafka-broker kafka:9094 --out-dir ./output/kafkaOutput --seed 42 --run-forever

#Step:4
docker exec -it spark-master /opt/spark/bin/spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/work-dir/scripts/spark_streaming_events_consumer.py   --mode stream --brokers kafka:9092 --raw-events-path /DB/consumed_events --products-path /lookupData/products.csv
