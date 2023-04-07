package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.ConfigLoader
import org.esgi.project.streaming.models.{AverageScoreForMovie, Like, LikeDeserializer, LikeSerializer, View, ViewDeserializer, ViewSerializer, ViewsWithScore}

import java.time.Duration
import java.util.{Properties, UUID}

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._


  val applicationName = s"kazaamovies-events-stream-app-${UUID.randomUUID}"
  private val kafkaProperties = ConfigLoader.loadPropertiesFile("kafka.properties")
  private val props: Properties = buildProperties
  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // Topics names
  val viewsTopicName: String = kafkaProperties.getProperty("views.topic")
  val likesTopicName: String = kafkaProperties.getProperty("likes.topic")

  // Store names
  val allTimeViewsCountStoreName: String = kafkaProperties.getProperty("store.name.all.time.view.count")
  val allTimesViewsPerCategoryCountStoreName: String = kafkaProperties.getProperty("store.name.all.time.view.count.per.category")
  val recentViewsPerCategoryCountStoreName: String = kafkaProperties.getProperty("store.name.last.five.minutes.view.count.per.category")
  val allTimesTenBestAverageScoreStoreName: String = kafkaProperties.getProperty("store.name.all.time.ten.best.average.scores")
  val movieTitlesStoreName: String = kafkaProperties.getProperty("store.name.movies.titles")


  //implicit Serde
  implicit val viewSerde: Serde[View] = toSerde[View]
  implicit val likeSerde: Serde[Like] = toSerde[Like]


  //topics sources
  val viewsTopicStream: KStream[String, View] = builder.stream[String, View](viewsTopicName)
  val likesTopicStream: KStream[String, Like] = builder.stream[String, Like](likesTopicName)

  // join
  val viewsAndLikesStream: KStream[String, ViewsWithScore] = likesTopicStream.join(viewsTopicStream)(
    joiner = { (like, view) => ViewsWithScore(_id = view.id, title = view.title, score = like.score) },
    windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60L))
  )

  // group by
  val viewsGroupedById: KGroupedStream[Long, View] = viewsTopicStream.groupBy((_, view) => view.id)
  val viewsGroupedByIdAndCategory: KGroupedStream[(Long, String), View] = viewsTopicStream.groupBy((_, view) => (view.id, view.view_category))

  // statistics computation
  val viewsOfAllTimes: KTable[Long, Long] = viewsGroupedById.count()(Materialized.as(allTimeViewsCountStoreName))
  val movieTitles: KTable[Long, String] = viewsGroupedById.aggregate("")(aggregator = {(_, view, _) => view.title})(Materialized.as(movieTitlesStoreName))
  val viewsOfAllTimesPerCategories: KTable[(Long, String), Long] = viewsGroupedByIdAndCategory.count()(Materialized.as(allTimesViewsPerCategoryCountStoreName))
  val viewsPerMoviesPerCategoriesLastFiveMinutes: KTable[Windowed[(Long, String)], Long] = viewsGroupedByIdAndCategory
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5L)).advanceBy(Duration.ofMinutes(1L)))
    .count()(Materialized.as(recentViewsPerCategoryCountStoreName))
  val averageScoreAllTimes: KTable[(Long, String), AverageScoreForMovie] = viewsAndLikesStream.groupBy((_, viewWithScore) => (viewWithScore._id, viewWithScore.title)).aggregate[AverageScoreForMovie](initializer = AverageScoreForMovie.empty
  )(aggregator = { (_, like, agg) => agg.increment(like.score) })(Materialized.as(allTimesTenBestAverageScoreStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    val stateListener: KafkaStreams.StateListener = (newState: KafkaStreams.State, oldState: KafkaStreams.State) => {
      println(s"KafkaStreams état changé de $oldState à $newState")
    }
    // Ajoutez l'écouteur d'état à l'instance KafkaStreams
    streams.setStateListener(stateListener)
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
    properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips")
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
