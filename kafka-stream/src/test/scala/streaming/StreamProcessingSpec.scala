package streaming
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{Likes, Views}
import org.scalatest.GivenWhenThen
import org.scalatest.funsuite.AnyFunSuite

import java.lang
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
    val views: List[Views] = events.map(_.view)

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(_.toTestRecord).asJava)

    Then("assert the count of all times views per movies")
    val expectedViewsPerMovies: Map[(Long, String), Long] = views.groupBy(view => (view._id, view.title)).map{ case (key, values) => (key, values.size)}
    val computedAllTimesViewsPerMovies = testDriver.getKeyValueStore[(Long, String), Long](StreamProcessing.allTimeViewsCountStoreName)
    expectedViewsPerMovies.foreach { case (movie, countViews) =>
      assert(computedAllTimesViewsPerMovies.get(movie) == countViews)}
    testDriver.close()
  }

  test("Validate all times count per category"){
    Given("a list of views and scores")
    val numberOfEvents: Int = Random.nextInt(10) + 1
    val events: List[GeneratedView] = Utils.generateEvents(numberOfEvents)
    val views: List[Views] = events.map(_.view)

    When("events are submitted to the cluster")
    val testDriver: TopologyTestDriver = new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewsPipeline = testDriver.createInputTopic(StreamProcessing.viewsTopicName,
      Serdes.longSerde.serializer, toSerde[Views].serializer)
    viewsPipeline.pipeRecordList(views.map(_.toTestRecord).asJava)

    Then("assert the count of all times views per movies per categories")
    val expectedViewsPerMoviesAndCategories: Map[Views, Long] = views.groupBy(view => view).map{ case (key, value) => (key, value.size)}
    val computedAllTimesViewsPerMoviesAndCategories = testDriver.getKeyValueStore[Views, Long](StreamProcessing.allTimesViewsPerCategoryCountStoreName)
    expectedViewsPerMoviesAndCategories.foreach{ case (view, countView) =>
    assert(computedAllTimesViewsPerMoviesAndCategories.get(view) == countView)}
  }

}
