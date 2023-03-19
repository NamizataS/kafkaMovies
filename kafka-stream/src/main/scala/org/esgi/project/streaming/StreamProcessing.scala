package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.ConfigLoader
import org.esgi.project.streaming.models.{Likes, Views}

import java.time.Duration
import java.util.{Properties, UUID}

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"kazaamovies-events-stream-app-${UUID.randomUUID}"
  private val kafkaProperties = ConfigLoader.loadPropertiesFile("kafka.properties")
  // Topics names
  val viewsTopicName: String = kafkaProperties.getProperty("views.topic")
  val likesTopicName: String = kafkaProperties.getProperty("likes.topic")

  // Store names
  val allTimeViewsCountStoreName: String = kafkaProperties.getProperty("store.name.all.time.view.count")
  val allTimesViewsPerCategoryCountStoreName: String = kafkaProperties.getProperty("store.name.all.time.view.count.per.category")
  val recentViewsPerCategoryCountStoreName: String = kafkaProperties.getProperty("store.name.last.five.minutes.view.count.per.category")
  private val props: Properties = buildProperties

  //implicit Serde
  implicit val viewsSerde: Serde[Views] = toSerde[Views]
  implicit val likesSerde: Serde[Likes] = toSerde[Likes]

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  //topics sources
  val viewsTopicStream: KStream[Long, Views] = builder.stream[Long, Views](viewsTopicName)

  //statistics computation
  val viewsGroupedByIdAndTitles: KGroupedStream[(Long, String), Views] = viewsTopicStream.groupBy((_, view) => (view._id, view.title))
  val viewsOfAllTimes: KTable[(Long, String), Long] = viewsGroupedByIdAndTitles.count()(Materialized.as(allTimeViewsCountStoreName))
  val viewsGroupedByIdTitlesAndCategory: KGroupedStream[Views, Views] = viewsTopicStream.groupBy((_, view) => view)
  val viewsOfAllTimesPerCategories: KTable[Views, Long] = viewsGroupedByIdTitlesAndCategory.count()(Materialized.as(allTimesViewsPerCategoryCountStoreName))
  val viewsPerMoviesPerCategoriesLastFiveMinutes: KTable[Windowed[Views], Long] = viewsGroupedByIdTitlesAndCategory
                                                  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5L)).advanceBy(Duration.ofMinutes(5L)))
                                                  .count()(Materialized.as(recentViewsPerCategoryCountStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    val properties = new Properties()
    val bootstrapServers = kafkaProperties.getProperty("bootstrap.servers")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
  def topology: Topology = builder.build()
}
