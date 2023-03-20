package streaming
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{Likes, Views}
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
      assert(computedAllTimesViewsPerMovies.get(movie) == countViews)}
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
  }

  test("Validate 5min count per category"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(10) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[(Views, Instant)] = events.map(event => (event.view, event.recordTimestamp))
    println(s"Events generated are ${views}")

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(view => view._1.toTestRecord(view._2)).asJava)

    Then("assert the count of the last 5 minutes views per movies per categories")
    val currentTime: Instant = OffsetDateTime.now(ZoneOffset.UTC).toInstant
    val expectedViewsPerMoviesAndCategoriesLastFiveMinutes: Map[Views, Long] = views.filter(view => ChronoUnit.MINUTES.between(view._2, currentTime) < 5)
                                                                                .groupBy(view => view._1)
                                                                                .map{ case (key, value) => (key, value.size)}
    println(s"expected are ${expectedViewsPerMoviesAndCategoriesLastFiveMinutes}")
    val computedViewsPerMoviesAndCategoriesLastFiveMinutes: WindowStore[Views, ValueAndTimestamp[Long]] = testDriver.getTimestampedWindowStore[Views, Long](StreamProcessing.recentViewsPerCategoryCountStoreName)
    computedViewsPerMoviesAndCategoriesLastFiveMinutes.fetchAll(currentTime.minus(Duration.ofMinutes(5)), currentTime).asScala.toList.foreach{
      row => println(s"movie is ${row.key.key().title} and category is ${row.key.key().viewsCategory} and count is ${row.value.value()} and timestamp is ${row.key.window().startTime().atOffset(ZoneOffset.UTC)}")
    }
    expectedViewsPerMoviesAndCategoriesLastFiveMinutes.foreach{ case (view, countView) =>
     val row = computedViewsPerMoviesAndCategoriesLastFiveMinutes.fetch(view, currentTime.minus(Duration.ofMinutes(5)), currentTime).asScala.toList
      row.headOption match {
        case Some(row) => assert(row.value.value() == countView, s"Values did not match for movie ${view.title} where $countView was expected and ${row.value.value()} was found for cat ${view.viewsCategory}")
        case None => assert(false, s"No data for movie ${view.title} and category ${view.viewsCategory} and count $countView")
      }
    }
  }

}
