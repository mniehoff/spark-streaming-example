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

import java.io.InputStream

import org.apache.kafka.clients.producer._
import java.util.HashMap

import scala.io.Source
import scala.util.Random

/**
 *
 */
object KafkaEventPublisher {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: KafkaEventPublisher <metadataBrokerList> <topic> ")
      System.exit(1)
    }

    val Array(brokers, topic) = args

    val stream = getClass.getResourceAsStream("/albums_title_year.csv")
    val albumFile = Source.fromInputStream(stream)
    val albums = albumFile.getLines().drop(1).map(_.split(",").toVector).toVector
    albumFile.close()

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while (true) {
      val bought = Random.shuffle(albums).take(50)
      bought
        .map {
          case Vector(title, year) => s"$title,$year"
          case _                   => throw new RuntimeException("Invalid format")
        }
        .foreach { str =>
          val message = new ProducerRecord[String, String](topic, null, str)
          producer.send(message)
        }

      Thread.sleep(200)
      System.out.println("Sent")
    }
  }
}
