package streaming
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{Likes, AverageScoreForMovie, Views, ViewsWithScore}
import org.scalatest.GivenWhenThen
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._
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
    val expectedViewsPerMoviesAndCategories: Map[(Long, String), Long] = views.groupBy(view => view._1).map{ case (key, value) => ((key._id, key.viewsCategory), value.size)}
    val computedAllTimesViewsPerMoviesAndCategories = testDriver.getKeyValueStore[(Long, String), Long](StreamProcessing.allTimesViewsPerCategoryCountStoreName)
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

    Then("assert the count of the last 5 minutes views per movies per categories")
    val expectedViewsPerMoviesAndCategoriesLastFiveMinutes: Map[(Long, String), Long] = views.filter{ case (_, timestamp) =>
                            val eventTime = timestamp.atOffset(ZoneOffset.UTC)
                            !eventTime.isBefore(fiveMinutesAgo) && !eventTime.isAfter(currentTime)}
            .groupBy(view => view._1)
            .map{ case (key, value) => ((key._id, key.viewsCategory), value.size)}

    val computedViewsPerMoviesAndCategoriesLastFiveMinutes: WindowStore[(Long, String), ValueAndTimestamp[Long]] = testDriver.getTimestampedWindowStore[(Long, String), Long](StreamProcessing.recentViewsPerCategoryCountStoreName)
    expectedViewsPerMoviesAndCategoriesLastFiveMinutes.foreach{ case (view, countView) =>
     val row = computedViewsPerMoviesAndCategoriesLastFiveMinutes.fetch(view, fiveMinutesAgo.toInstant, currentTime.toInstant).asScala.toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == countView, s"Values did not match for movie id ${view._1} and category ${view._2} where $countView was expected and ${row.value.value()} ")
        case None => assert(false, s"No data for movie id num ${view._1} and category ${view._2}")
      }
    }
    testDriver.close()
  }

  test("Validate average score all time") {
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(16) + 10
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    val likes: List[(Likes, Instant)] = events.map(event => (event.like, event.recordTimestamp))
    val epsilon: Double = 1e-12

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    val likesPipeline = testDriver.createInputTopic(StreamProcessing.likesTopicName,
      Serdes.longSerde.serializer, toSerde[Likes].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)
    likesPipeline.pipeRecordList(likes.map(like => like._1.toTestRecord(like._2)).asJava)

    Then("assert the average score for each movie generated")
    val expectedAverageScorePerMovies: Map[(Long, String), Double] = events.map(event => new ViewsWithScore(event.view._id, event.view.title, event.view.viewsCategory, event.like.score))
      .groupBy(viewWithScore => (viewWithScore._id, viewWithScore.title)).map { case (key, viewWithScore) =>
      val averageScore = viewWithScore.map(_.score).sum / viewWithScore.size
      (key, averageScore)
    }
    val computedAverageScorePerMovies = testDriver.getKeyValueStore[(Long, String), AverageScoreForMovie](StreamProcessing.allTimesTenBestAverageScoreStoreName)
    expectedAverageScorePerMovies.foreach { case (movie, averageScore) =>
      assert(math.abs(computedAverageScorePerMovies.get(movie).averageScore - averageScore) < epsilon )
    }
    testDriver.close()
  }

}
