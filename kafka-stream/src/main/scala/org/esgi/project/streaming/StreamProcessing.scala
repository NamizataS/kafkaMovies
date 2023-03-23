package org.esgi.project.streaming

import akka.japi.Predicate
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{JoinWindows, SessionWindows, TimeWindows, Transformer, TransformerSupplier, Windowed}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.ConfigLoader
import org.esgi.project.streaming.models.{Likes, MeanScoreForMovie, Views, ViewsWithScore}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
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
  val allTimesTenBestAverageScoreStoreName: String = kafkaProperties.getProperty("store.name.all.time.ten.best.average.scores")
  private val props: Properties = buildProperties

  //implicit Serde
  implicit val viewsSerde: Serde[Views] = toSerde[Views]
  implicit val likesSerde: Serde[Likes] = toSerde[Likes]

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  //topics sources
  val viewsTopicStream: KStream[Long, Views] = builder.stream[Long, Views](viewsTopicName)
  val likesTopicStream: KStream[Long, Likes] = builder.stream[Long, Likes](likesTopicName)
  //rajouter un builder.table pour faire un join sur une table et pas un stream
  val viewsAndLikes: KStream[Long, ViewsWithScore] = likesTopicStream.join(viewsTopicStream)(
    joiner = {(like, view) => ViewsWithScore(_id = like._id, title = view.title, viewCategory = view.viewsCategory, score = like.score)
    },
    windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10))
  )

  //statistics computation
  val viewsGroupedByIdAndTitles: KGroupedStream[(Long, String), Views] = viewsTopicStream.groupBy((_, view) => (view._id, view.title))
  val viewsAndLikesGroupByIdAndTitles: KGroupedStream[(Long, String), ViewsWithScore] = viewsAndLikes.groupBy((_, viewAndLike) => (viewAndLike._id, viewAndLike.title))
  val viewsOfAllTimes: KTable[(Long, String), Long] = viewsGroupedByIdAndTitles.count()(Materialized.as(allTimeViewsCountStoreName))
  val viewsGroupedByIdTitlesAndCategory: KGroupedStream[Views, Views] = viewsTopicStream.groupBy((_, view) => view)
  val viewsOfAllTimesPerCategories: KTable[Views, Long] = viewsGroupedByIdTitlesAndCategory.count()(Materialized.as(allTimesViewsPerCategoryCountStoreName))
  val startTime: Instant = OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofMinutes(5)).toInstant
  //val filteredStream: KStream[Long, Views] = viewsTopicStream.filter((key, value) => value.)
  //val viewsPerMoviesPerCategoriesLastFiveMinutes: KTable[Windowed[Views], Long] = ???
  val viewsPerMoviesPerCategoriesLastFiveMinutes: KTable[Windowed[Views], Long] = viewsGroupedByIdTitlesAndCategory
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(recentViewsPerCategoryCountStoreName))

  val averageScoreAllTimes:KTable[(Long, String), MeanScoreForMovie] = viewsAndLikesGroupByIdAndTitles.aggregate[MeanScoreForMovie](
    initializer = MeanScoreForMovie.empty
  )(aggregator = {(_, viewWithScore, agg) => agg.increment(viewWithScore.score)})(Materialized.as(allTimesTenBestAverageScoreStoreName))


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
