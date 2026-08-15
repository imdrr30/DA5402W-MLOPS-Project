producer ---> Model Building pipeline -> 
		 ---> Analytics Pipeline
		 ---> Prediction Pipeline --> model_prediction consumer -> Show to user
		 ---> Backend Process Triggers(yet to figure out)


=============================

# Clean up unwanted files and docker fresh start, Otherwise skip to step 6

1. List down the kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

(Delete the topics one by one)
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

2. Remove the DB files, try docker command, if it doesn't work try sudo

docker exec -it spark-master bash -c "rm -rf /DB/consumed_events/* /DB/topic_dumps/* /DB/metrics/* /DB/analytics/*"
sudo rm -rf outputs/* checkpoints/* spark-warehouse .metastore_db
sudo rm -rf /DB/consumed_events/* /DB/topic_dumps/* /DB/metrics/*
sudo find . -name "*.crc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

3. Down the container
docker compose -f docker_compose.yaml down

(OR, stop and delete everything related to the compose project)
docker compose -f docker_compose.yaml down --rmi all -v --remove-orphans

4. Full cleanup, Remove unused Docker resources, only if required !!...THIS WILL REMOVE ALL THE CONTAINERS PULLED...!!
docker system prune -a --volumes -f

------------------

# Fresh start using docker

6. Build fresh images, installations happen here
docker compose -f docker_compose.yaml build --no-cache

7. Fire up the services one by one, and check logs if needed

docker compose -f docker_compose.yaml up -d postgres
docker compose -f docker_compose.yaml logs postgres

(Create kafka topics)
docker compose -f docker_compose.yaml up -d topic-init
docker compose -f docker_compose.yaml logs topic-init

docker compose -f docker_compose.yaml up airflow-init
docker compose -f docker_compose.yaml logs airflow-init

(Start streaming script)
docker compose -f docker_compose.yaml up -d spark
docker compose -f docker_compose.yaml logs -f spark

(Wait for some time, Check if all the containers are running)
docker compose -f ./docker_compose.yaml ps

8. Fireup airflow webserver and verify UI at localhost:8080 and check logs
docker compose -f docker_compose.yaml up -d airflow-webserver airflow-scheduler
docker compose -f docker_compose.yaml logs airflow-scheduler

9. Fire up the producer, use --kafka-broker as kafka:9094 if running from inside docker
python ./src/kafkaScripts/launch_producer.py --n-users 1000 --n-products 200 --batch-size 500 --interval-seconds 5 --kafka-broker localhost:9094 --out-dir ./outputs/kafkaOutput --seed 42 --run-forever

------------------
