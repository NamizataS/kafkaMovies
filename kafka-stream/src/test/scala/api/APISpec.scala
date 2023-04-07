package api

import api.Tools.Converters.{LikesToTestRecord, ViewToTestRecord}
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.streams.KafkaStreams
import org.esgi.project.streaming.models.{Like, View, ViewsWithScore}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, GivenWhenThen}
import org.scalatest.funsuite.AnyFunSuite
import streaming.Tools.Models.GeneratedView
import streaming.Tools.Utils
import org.esgi.project.api.API
import org.esgi.project.streaming.StreamProcessing
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.apache.kafka.streams.scala.serialization.Serdes
import org.esgi.project.api.models.{MovieAverageScore, Stats, StatsDetails, ViewsPerMovies}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}


import java.io.File
import scala.jdk.CollectionConverters._
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import java.util.Properties
import scala.util.Random

class APISpec extends AnyFunSuite with GivenWhenThen with BeforeAndAfterAll with PlayJsonSupport with BeforeAndAfterEach with EmbeddedKafka {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  // val cluster = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
  var adminClient: AdminClient = _
  var streamApp: KafkaStreams = _
  var api: API = _

  val adminClientProps = new Properties()
  val streamsConfig = new Properties()
  val producerProps = new Properties()
  streamsConfig.put("application.id", "test")
  streamsConfig.put("auto.offset.reset", "latest")

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${config.kafkaPort}")
    streamsConfig.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${config.kafkaPort}")
    adminClient = AdminClient.create(adminClientProps)
    val viewTopic = new NewTopic(StreamProcessing.viewsTopicName, 1, 1.toShort)
    val likeTopic = new NewTopic(StreamProcessing.likesTopicName, 1, 1.toShort)
    adminClient.createTopics(List(viewTopic, likeTopic).asJava)

    streamApp = new KafkaStreams(StreamProcessing.topology, streamsConfig)
    api = new API(streamApp)

    streamApp.start()
    while (!List(KafkaStreams.State.RUNNING, KafkaStreams.State.ERROR).contains(streamApp.state)) {
      Thread.sleep(1000)
    }

    if (streamApp.state == KafkaStreams.State.RUNNING) {
      println("Stream app started")
    } else {
      println("Stream app failed to start")
      throw new RuntimeException("Stream app failed to start")
    }

  }

  override def afterAll(): Unit = {
    streamApp.close()
    // wait for the topology to be stopped
    while (!(streamApp.state == KafkaStreams.State.NOT_RUNNING)) {
      Thread.sleep(1000)
    }

    adminClient.close()
    EmbeddedKafka.stop()
    adminClientProps.remove("bootstrap.servers")
    producerProps.remove("bootstrap.servers")
    streamsConfig.remove("bootstrap.servers")
  }

  test("Validate ViewsPerMovies list") {
    Given("A list of views and score")
    val numberOfEvents: Int = Random.nextInt(30) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(View, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    val likes: List[(Like, Instant)] = events.map(event => (event.like, event.recordTimestamp))
    val viewsProducer = new KafkaProducer[String, View](producerProps, Serdes.stringSerde.serializer(), toSerde[View].serializer())
    val likesProducer = new KafkaProducer[String, Like](producerProps, Serdes.stringSerde.serializer(), toSerde[Like].serializer())
    val toDate = OffsetDateTime.now(ZoneOffset.UTC)
    val fromDate = toDate.minus(Duration.ofMinutes(5L))

    When("events are submitted to the cluster")
    views.foreach(event => viewsProducer.send(event._1.toRecord(StreamProcessing.viewsTopicName, event._2)))
    likes.foreach(event => likesProducer.send(event._1.toRecord(StreamProcessing.likesTopicName, event._2)))
    viewsProducer.flush()
    likesProducer.flush()

    viewsProducer.close()
    likesProducer.close()

    Then("Check if views per movies is good")

    val countTotalViews = views.groupBy(view => view._1.id).map { case (key, value) => (key, value.size.toLong) }
    val countTotalViewsPerCategory = views.groupBy(view => view._1).map { case (key, value) => ((key.id, key.view_category), value.size.toLong) }
    val countRecentTotalViewsPerCategory = views.filter { case (_, timestamp) =>
      val eventTime = timestamp.atOffset(ZoneOffset.UTC)
      !eventTime.isBefore(fromDate) && !eventTime.isAfter(toDate)
    }
      .groupBy(view => view._1)
      .map { case (key, value) => ((key.id, key.view_category), value.size.toLong) }

    val expectedViewsPerMovies: List[List[ViewsPerMovies]] = Utils.moviesTitles.zipWithIndex.map { case (title, index) =>
      val totalViews = countTotalViews.getOrElse(index.toLong, 0L)
      val movieTitle: String = totalViews match {
        case _ if totalViews > 0L => title
        case _ => ""
      }
      val pastViews = new StatsDetails(start_only = countTotalViewsPerCategory.getOrElse((index.toLong, "start_only"), 0L),
        half = countTotalViewsPerCategory.getOrElse((index.toLong, "half"), 0L), full = countTotalViewsPerCategory.getOrElse((index.toLong, "full"), 0L))
      val recentViews = new StatsDetails(start_only = countRecentTotalViewsPerCategory.getOrElse((index.toLong, "start_only"), 0L),
        half = countRecentTotalViewsPerCategory.getOrElse((index.toLong, "half"), 0L), full = countRecentTotalViewsPerCategory.getOrElse((index.toLong, "full"), 0L))
      List(new ViewsPerMovies(_id = index.toLong, title = movieTitle, total_view_count = totalViews,
        stats = new Stats(past = pastViews, last_five_minutes = recentViews)))
    }

    expectedViewsPerMovies.zipWithIndex.foreach { case (viewsMovie, index) =>
      val apiResults: List[ViewsPerMovies] = api.viewsPerMovies(from = fromDate, to = toDate, id = index.toLong)
      assert(apiResults.length == 1)
      assert(viewsMovie == apiResults)
    }
  }

  test("Validate top 10 best views and scores") {
    Given("A list of views and score")
    val numberOfEvents: Int = Random.nextInt(16) + 10
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(View, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    val likes: List[(Like, Instant)] = events.map(event => (event.like, event.recordTimestamp))
    val viewsProducer = new KafkaProducer[String, View](producerProps, Serdes.stringSerde.serializer(), toSerde[View].serializer())
    val likesProducer = new KafkaProducer[String, Like](producerProps, Serdes.stringSerde.serializer(), toSerde[Like].serializer())

    When("events are submitted to the cluster")
    views.foreach(event => viewsProducer.send(event._1.toRecord(StreamProcessing.viewsTopicName, event._2)))
    likes.foreach(event => likesProducer.send(event._1.toRecord(StreamProcessing.likesTopicName, event._2)))
    viewsProducer.flush()
    likesProducer.flush()

    viewsProducer.close()
    likesProducer.close()

    Then("Check if best score is good")
    val expectedAverageScorePerMovies: List[MovieAverageScore] = events.map(event => new ViewsWithScore(event.view.id, event.view.title, event.like.score))
      .groupBy(viewWithScore => viewWithScore._id).map { case (key, viewWithScore) =>
      val averageScore = viewWithScore.map(_.score).sum / viewWithScore.size
      (key, averageScore)
    }.toList.sortBy { case (_, value) => -value }.take(10).map(movie => new MovieAverageScore(id = movie._1,
      title = Utils.moviesTitles(movie._1.toInt), score = movie._2))
    val apiResults = api.tenBestOrWorseScore(best = true)
    assert(expectedAverageScorePerMovies.map(expected => (expected.id, expected.title)) == apiResults.map(computed => (computed.id, computed.title)))
  }
}
