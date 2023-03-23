package streaming
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{Likes, MeanScoreForMovie, Views, ViewsWithScore}
import org.scalatest.GivenWhenThen
import org.scalatest.funsuite.AnyFunSuite

import java.lang
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import scala.util.Random

class StreamProcessingSpec extends AnyFunSuite with GivenWhenThen with PlayJsonSupport {
  import Tools.Utils
  import Tools.Models.GeneratedView
  import Tools.Converters._
  test("Validate all times count"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(10) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)

    Then("assert the count of all times views per movies")
    val expectedViewsPerMovies: Map[(Long, String), Long] = views.groupBy(view => (view._1._id, view._1.title)).map{ case (key, values) => (key, values.size)}
    val computedAllTimesViewsPerMovies = testDriver.getKeyValueStore[(Long, String), Long](StreamProcessing.allTimeViewsCountStoreName)
    expectedViewsPerMovies.foreach { case (movie, countViews) =>
      assert(computedAllTimesViewsPerMovies.get(movie) == countViews)
    }
    testDriver.close()
  }

  test("Validate all times count per category"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(10) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)

    Then("assert the count of all times views per movies per categories")
    val expectedViewsPerMoviesAndCategories: Map[Views, Long] = views.groupBy(view => view._1).map{ case (key, value) => (key, value.size)}
    val computedAllTimesViewsPerMoviesAndCategories = testDriver.getKeyValueStore[Views, Long](StreamProcessing.allTimesViewsPerCategoryCountStoreName)
    expectedViewsPerMoviesAndCategories.foreach{ case (view, countView) =>
    assert(computedAllTimesViewsPerMoviesAndCategories.get(view) == countView)}
    testDriver.close()
  }

  test("Validate 5min count per category"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(16) + 10
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    val currentTime: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
    val fiveMinutesAgo = currentTime.minus(Duration.ofMinutes(5))

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)
    //Thread.sleep(Duration.ofMinutes(1).toMillis)

    Then("assert the count of the last 5 minutes views per movies per categories")
    val expectedViewsPerMoviesAndCategoriesLastFiveMinutes: Map[Views, Long] = views.filter{ case (_, timestamp) =>
                            val eventTime = timestamp.atOffset(ZoneOffset.UTC)
                            !eventTime.isBefore(fiveMinutesAgo) && !eventTime.isAfter(currentTime)}
            .groupBy(view => view._1)
            .map{ case (key, value) => (key, value.size)}

    val computedViewsPerMoviesAndCategoriesLastFiveMinutes: WindowStore[Views, ValueAndTimestamp[Long]] = testDriver.getTimestampedWindowStore[Views, Long](StreamProcessing.recentViewsPerCategoryCountStoreName)
    expectedViewsPerMoviesAndCategoriesLastFiveMinutes.foreach{ case (view, countView) =>
     val row = computedViewsPerMoviesAndCategoriesLastFiveMinutes.fetch(view, fiveMinutesAgo.toInstant, currentTime.toInstant).asScala.toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == countView, s"Values did not match for movie ${view.title} where $countView was expected and ${row.value.value()} was found for cat ${view.viewsCategory}")
        case None => assert(false, s"No data for movie ${view.title} and category ${view.viewsCategory} and count $countView")
      }
    }
    testDriver.close()
  }

  test("Validate mean score"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(10) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    val likes: List[(Likes, Instant)] = events.map(event => (event.like, event.recordTimestamp))
    println(s"events generated are $events")
    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    val likesPipeline = testDriver.createInputTopic(StreamProcessing.likesTopicName,
      Serdes.longSerde.serializer, toSerde[Likes].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)
    likesPipeline.pipeRecordList(likes.map(like => like._1.toTestRecord(like._2)).asJava)

    Then("assert the mean score for each movie generated")
    val expectedMeanScorePerMovies: Map[(Long, String), Double] = events.map(event => new ViewsWithScore(event.view._id, event.view.title, event.view.viewsCategory, event.like.score))
      .groupBy(viewWithScore => (viewWithScore._id, viewWithScore.title)).map{ case (key, viewWithScore) =>
    val meanScore = viewWithScore.map(_.score).sum / viewWithScore.size
      (key, meanScore)}
    val computedMeanScorePerMovies = testDriver.getKeyValueStore[(Long, String), MeanScoreForMovie](StreamProcessing.allTimesTenBestAverageScoreStoreName)
    println("test driver value")
    testDriver.getKeyValueStore[(Long, String), MeanScoreForMovie](StreamProcessing.allTimesTenBestAverageScoreStoreName).all().asScala.toList
      .foreach(record => println(s"movie is ${record.key._2} and meanScore is ${record.value.meanScore}"))
    println("Expected values")
    expectedMeanScorePerMovies.foreach(record => println(s"movie is ${record._1._2} and meanScore is ${record._2}"))
    expectedMeanScorePerMovies.foreach{ case (movie, meanScore) =>
      assert(computedMeanScorePerMovies.get(movie).meanScore == meanScore)}
    testDriver.close()
  }

}
