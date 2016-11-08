# Apache Spark Kafka Streaming

## Prerequisite
JAVA Version 7  
SBT Version 0.13.11  
SCALA Version 2.10.5  
SPARK Version 1.6.1  
MAVEN 3.3.9  
DOCKER 1.11.1  
DOCKER COMPOSE 1.7.0  
 
The example application demonstrate Apache Spark Streaming with Kafka.The application runs inside Docker containers.

Below are the steps to setup and run the example.

1.Clone these repositories

```
git clone https://github.com/alvinhenrick/awesome-data
git clone https://github.com/alvinhenrick/spark-wikistream-example.git
```

2.Get into _awesome-data_ directory and build docker images by running the following shell script

`./buildall.sh`

3.Get into _spark-wikistream-example_ directory and do the maven build to create uber jar

`mvn clean package`

4.Start all containers by running the following command.

```
docker network create hadoop
docker-compose up -d
```

5.Open two separate terminals and change directory to _spark-wikistream-example_ 

6.Start two containers by running the following shell script

*  On terminal one type `./enter-spark.sh` and hit enter
*  On terminal two type `./enter-spark.sh` and hit enter

7.Once inside the containers run the following command 
 
 `cd /data`
 
8.Once inside the _data_ directory run following shell scripts in order
 
*  On terminal one type `./start-consumer.sh` and hit enter
*  On terminal two type `./start-producer.sh` and hit enter
    

