/*
 * Copyright 2016 Matthias Niehoff
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.codecentric.spark.streaming.example

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import java.util.HashMap
import org.apache.kafka.clients.producer._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * Varios experiments with state and saving to Cassandra
 */
object KafkaStreamingWithState {

  final case class Album(title: String, year: Int, performer: String, genre: String)
  final case class AlbumWithCount(title: String, year: Int, performer: String, genre: String, count: Long)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)

    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: KafkaStreamingBillboard <brokers> <topics> <cassandraHost>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <cassandraHost> is a list of one ore more Cassandra nodes
           |
        """.stripMargin
      )
      System.exit(1)
    }

    val Array(brokers, topics, cassandraHost) = args

    val conf = new SparkConf()
      .setAppName("Kafka Billboard Charts")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.keep_alive_ms", "60000") // prevent cassandra connection from being closed after every write
    val ssc = new StreamingContext(conf, Seconds(1))

    // State definition
    val initialState = ssc
      .cassandraTable[(String, String, Long)]("music", "charts_with_diff_state").select("title", "year", "count")
      .map { case (title, year, count) => ((title, year), count) }

    val countByTitleAndYear = StateSpec.function(updateState _).initialState(initialState)

    // Kafka stream
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet
    val sales = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet);

    val parsedStream = sales.map(_._2.split(",").toVector).map {
      case Vector(title, year) => (title, year)
      case _                   => throw new RuntimeException("Invalid format");
    }

    // Update the state with the information from the stream.
    val updatedStream = parsedStream
      .map { case (title, year) => ((title, year), 1) }
      .mapWithState(countByTitleAndYear)

    updatedStream.cache() // updatedStreams is used for multiple actions

    // updatedStream contains all the entries emmited by trackStateFunc (one entry for each entry in the original dstream. might have multiple entries per key)
    // use this to update the state in Cassandra.
    // once sorted and printed
    updatedStream
      .transform(rdd => rdd.sortBy { case ((_, _), count) => -count })
      .print(10)

    // once saved to Cassandra
    updatedStream
      .map { case ((title, year), count) => (title, year, count) }
      .saveToCassandra("music", "charts_with_diff_state")

    // the complete current state. one entry per key
    // joined with more album Informations stored in Cassandra
    updatedStream
      .stateSnapshots()
      .map { case ((title, year), count) => (title, year, count) }
      .repartitionByCassandraReplica("music", "albums") // ensure that all Spark data resides on the same node as the cassandra replica
      .joinWithCassandraTable[Album]("music", "albums")
      .map { case ((_, _, count), album) => AlbumWithCount(album.title, album.year, album.performer, album.genre, count) }
      .saveToCassandra("music", "charts_with_complete_state")

    ssc.checkpoint(System.getProperty("java.io.tmpdir"))
    ssc.start()
    ssc.awaitTermination()
  }

  def updateState(batchTime: Time, key: (String, String), value: Option[Int], state: State[Long]): Option[((String, String), Long)] = {
    val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    val output = (key, sum)
    state.update(sum)
    Some(output)
  }
}
