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
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming._
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import java.util.HashMap
import org.apache.kafka.clients.producer._

/**
 * This app
 * - Reads albumDownloads from Kafka
 * - Counts the Downloads in a specific time window
 * - Joins the incoming data with additional data saved in Cassandra
 * - Maintains a state to get percentages change between every time window
 * - prints the top 10 download with state to the console
 * - saves the current window data to Cassandra
 */
object KafkaStreamingBillboard {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)
    Logger.getLogger("kafka.utils").setLevel(Level.WARN)

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

    val checkpointDir = System.getProperty("java.io.tmpdir")

    // Encapsulated in a function for usage in StreamingContext.getOrCreate
    def createSparkContext(): StreamingContext = {
      val Array(brokers, topics, cassandraHost) = args
      val conf = new SparkConf()
        .setAppName("Kafka Billboard Charts")
        .setMaster("local[2]")
        .set("spark.cassandra.connection.host", cassandraHost)
        .set("spark.cassandra.connection.keep_alive_ms", "60000") // prevent cassandra connection from being closed after every write
      val ssc = new StreamingContext(conf, Seconds(1))

      // Define the State
      val stateFromCassandra = ssc
        .cassandraTable[(String, Int, Long)]("music", "billboard_charts")
        .map { case (title, year, count) => ((title, String.valueOf(year)), (count, 0d)) }

      val billboardState = StateSpec.function(updateBillboardState _).initialState(stateFromCassandra)

      // Read from Kafka
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      val sales = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      val parsedStream = sales.map(_._2.split(",")).map {
        case Array(title, year) => ((title, year), 1)
        case _                  => throw new RuntimeException("Invalid format");
      }

      // Reduce By Window
      val windowedStream = parsedStream
        .reduceByKeyAndWindow(
          { (countA, countB) => countA + countB }, { (countA, countB) => countA - countB },
          Seconds(60), // Window Duration
          Seconds(30)
        ) // Slide Duration
      // process the last 60s every 30s

      // update the stream and the state
      // as the updatedStream is used for multiple actions (saveToCassandra, print) it should be cached to prevent recomputation.
      val updatedStream = windowedStream.mapWithState(billboardState).cache()

      // save updated stream to cassandra
      updatedStream
        .map { case ((title, year), (count, _)) => (title, year, count) }
        .saveToCassandra("music", "billboard_charts")

      // join with additional data from cassandra
      val joinedStream = updatedStream
        .map { case ((title, year), (count, percentage)) => (title, year, count, percentage) }
        .joinWithCassandraTable[(String, String)]("music", "albums", selectedColumns = SomeColumns("performer", "genre"))

      // Sort and print to console with additional/joined information
      joinedStream.transform(rdd =>
        rdd.map { case ((title, year, count, percentage), (performer, genre)) => (count, (percentage, title, year, performer, genre)) }
          .sortByKey(false)
          .map { case (count, (percentage, title, year, performer, genre)) => (count, "%+.2f%%".format(percentage).replace("NaN%", "NEW!"), title, year, performer, genre) })
        .print(10)

      ssc.checkpoint(checkpointDir)
      ssc
    }

    val context = StreamingContext.getOrCreate(checkpointDir, createSparkContext _)
    context.start()
    context.awaitTermination()
  }

  def updateBillboardState(batchTime: Time, key: (String, String), value: Option[Int], state: State[(Long, Double)]): Option[((String, String), (Long, Double))] = {
    var billboard = (value.getOrElse(0).toLong, Double.NaN)
    if (state.exists && state.get._1 != 0) {
      val change = (billboard._1 - state.get._1) * 100 / state.get._1
      billboard = (value.getOrElse(0).toLong, change)
    }
    state.update(billboard)
    Some(key, billboard)
  }

}
