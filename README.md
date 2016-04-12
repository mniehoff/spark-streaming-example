# spark-streaming-example #

This repository contains example code for spark-streaming applications with window and state operations. The application will receive the data from a Kafka broker and will save the result in a Cassandra database.

With the following commands the apps can be run on your local computer in Spark local mode (master,driver,worker,executors all in one jvm). To run this on a spark cluster you have to create an assembly jar and submit this to the cluster.

## Setup

You can run the necessary tools (Kafka, Cassandra) using docker or any other way.

### Kafka

Set up one node, i.e. using docker:

```
docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka
```

Create the topic albumDownloads
```
docker exec -i -t `docker ps | grep spotify/kafka | awk ' { print $1 } '` /opt/kafka_2.11-0.8.2.1/bin/kafka-topics.sh "--create --zookeeper `docker-machine ip \`docker-machine active\`` --replication-factor 1 --partitions 1 --topic albumDownloads"
```

### Cassandra

Set up one node, i.e. using docker:

```
docker run -d -p 9042:9042 cassandra:2.2
```

## Prepare Cassandra and Send Events

Start sbt (executed in the root of this Git repository)

This will start the system installation of sbt if present. Otherwise it will download the sbt-launcher jar into the sbt directory and will then launch sbt using the launcher jar.

```
chmod a+x sbt/sbt
./sbt/sbt
```

### Initialize Cassandra

This will create a keypsace music with some tables. Furthermore it will write some data into the album table using Spark.

```
runMain de.codecentric.spark.streaming.example.CassandraInitializer <<yourCassandraHost>>
````

### Send Events
```
runMain de.codecentric.spark.streaming.example.KafkaEventPublisher <<yourKafkaHost:9092>> albumDownloads
```

## Run the Streaming Jobs

Start another sbt session (executed in the root of this Git repository)

### Run BillboardCharts Job

Generates BillboardCharts as described in the German Java Magazin

```
runMain de.codecentric.spark.streaming.example.KafkaStreamingBillboard <<yourKafkaHost:9092>> albumDownloads <<yourCassandaHost>>
```

### Run ChartsWithState Job
```
runMain de.codecentric.spark.streaming.example.KafkaStreamingWithState <<yourKafkaHost:9092>> albumDownloads <<yourCassandaHost>>

```

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with any pull requests, please state that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.

## License ##

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
