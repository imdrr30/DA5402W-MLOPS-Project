producer ---> Model Building pipeline -> 
		 ---> Analytics Pipeline
		 ---> Prediction Pipeline --> model_prediction consumer -> Show to user
		 ---> Backend Process Triggers(yet to figure out)


=============================

Fresh start using docker

# step 0.1 : Stop and delete everything related to the compose project
docker compose -f docker_compose.yaml down --rmi all -v --remove-orphans

# step 0.2 : Remove unused Docker resources
docker system prune -a --volumes -f

# step 0.3 : Build fresh images, installations happen here
docker compose -f docker_compose.yaml build --no-cache

# step 0.4 : Start the stack
docker compose -f docker_compose.yaml up -d

# step 0.5 : Verify running images
docker ps
docker logs kafka


=============================


# STEP 0 : installations happen here
docker compose -f docker_compose.yaml build --no-cache

# STEP 1
docker compose -f docker_compose.yaml up -d

# STEP 2
python ./src/kafkaScripts/topicCreator.py

# STEP 3 : use --kafka-broker as localhost:9094 if running from outside docker
python ./src/kafkaScripts/launch_producer.py --n-users 1000 --n-products 200 --batch-size 500 --interval-seconds 5 --kafka-broker kafka:9094 --out-dir ./outputs/kafkaOutput --seed 42 --run-forever

# STEP 4
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/work-dir/scripts/spark_streaming_consumer.py \
  --mode stream \
  --brokers kafka:9092 \
  --raw-events-path /DB/consumed_events \
  --products-path /lookupData/products.csv \
  --dump-raw-topics --dump-path /DB/topic_dumps \
  --enable-console-metrics --metrics-sink csv --metrics-path /DB/metrics


=============================


# Clean up unwanted files

docker exec -it spark-master bash -c "rm -rf /chk /DB/consumed_events /DB/topic_dumps /DB/metrics"
sudo rm -rf DB/* outputs/* output/* checkpoints/* spark-warehouse .metastore_db
docker exec -it spark-master rm -rf /DB/consumed_events
sudo find . -name "*.crc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete


=============================



mkdir airflow
cd airflow
mkdir -p logs plugins outputs DB

docker compose -f docker_compose.yaml down
sudo chown -R 50000:0 logs plugins outputs DB src/airflowScripts/dags

docker compose -f docker_compose.yaml up -d postgres
docker compose -f docker_compose.yaml up airflow-init
docker compose -f docker_compose.yaml up -d airflow-webserver airflow-scheduler
docker compose -f docker_compose.yaml logs airflow-scheduler --tail=50




docker compose -f ./docker_compose.yaml build --no-cache topic-init
docker compose -f ./docker_compose.yaml up -d topic-init
docker compose -f ./docker_compose.yaml logs topic-init
docker compose -f ./docker_compose.yaml ps
<!-- docker compose -f ./docker_compose.yaml build --no-cache topic-init -->
docker compose -f ./docker_compose.yaml up -d spark
docker compose -f ./docker_compose.yaml logs -f spark

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

------------------

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic user-events

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic recommendation-actions

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic notification-events


sudo rm -rf DB/* outputs/* output/* checkpoints/* spark-warehouse .metastore_db
sudo find . -name "*.crc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete


docker compose -f docker_compose.yaml build --no-cache
docker compose -f docker_compose.yaml down

docker compose -f docker_compose.yaml up -d postgres
docker compose -f docker_compose.yaml logs postgres
docker compose -f docker_compose.yaml up -d topic-init
docker compose -f docker_compose.yaml logs topic-init
docker compose -f docker_compose.yaml up airflow-init
docker compose -f docker_compose.yaml logs airflow-init
docker compose -f docker_compose.yaml up -d spark
docker compose -f docker_compose.yaml logs -f spark

docker compose -f docker_compose.yaml up -d airflow-webserver airflow-scheduler


docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list


------------------

docker exec spark-master rm -rf /DB/topic_dumps/user_events
docker exec spark-master rm -rf /DB/topic_dumps/recommendation_events
docker exec spark-master rm -rf /DB/topic_dumps/notification_events

docker exec spark-master rm -rf /checkpoints/dump_user_events
docker exec spark-master rm -rf /checkpoints/dump_recommendation_events
docker exec spark-master rm -rf /checkpoints/dump_notification_events

