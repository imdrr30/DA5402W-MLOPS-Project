producer ---> Model Building pipeline -> 
		 ---> Analytics Pipeline
		 ---> Prediction Pipeline --> model_prediction consumer -> Show to user
		 ---> Backend Process Triggers(yet to figure out)

=============================

#STEP:1
docker compose -f docker_compose.yaml up -d

#STEP:2
pip install kafka-python
python ./src/kafkaScripts/topicCreator.py

#Step:3
python ./src/kafkaScripts/launch_producer.py --n-users 1000 --n-products 200 --batch-size 500 --interval-seconds 5 --kafka-broker kafka:9094 --out-dir ./output/kafkaOutput --seed 42 --run-forever

#Step:4
docker exec -it spark-master /opt/spark/bin/spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/work-dir/scripts/spark_streaming_events_consumer.py   --mode stream --brokers kafka:9092 --raw-events-path /DB/consumed_events --products-path /lookupData/products.csv

=============================

Fresh start using docker

# step 1.1 : Stop and delete everything related to the compose project
docker compose -f docker_compose.yaml down --rmi all -v --remove-orphans

# step 1.2 : Remove unused Docker resources
docker system prune -a --volumes -f

# step 1.3 : Build fresh images, installations happen here
docker compose -f docker_compose.yaml build --no-cache

# step 1.4 : Start the stack
docker compose -f docker_compose.yaml up -d

# step 1.5 : Verify running images
docker ps
docker logs kafka

=============================

# Clean up unwanted files

sudo rm -rf DB/* outputs/* output/* checkpoints/* spark-warehouse .metastore_db
docker exec -it spark-master rm -rf /DB/consumed_events
sudo find . -name "*.crc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete




