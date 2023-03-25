package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, Stats, TemporalStats, ViewsPerMovies, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{MeanLatencyForURL, Views}

import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters._

class API(streamApp: KafkaStreams){
  def viewsPerMovies(from: OffsetDateTime, to: OffsetDateTime, id: Long): List[ViewsPerMovies] = {
    val parametersAllTimesViewCount = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimeViewsCountStoreName, QueryableStoreTypes.keyValueStore[(Long, String), Long]())
    val parametersAllTimesViewCountPerCategory = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimesViewsPerCategoryCountStoreName, QueryableStoreTypes.keyValueStore[(Long, String), Long]())
    val parametersLastFiveMinutesCountPerCategory = StoreQueryParameters.fromNameAndType(StreamProcessing.recentViewsPerCategoryCountStoreName, QueryableStoreTypes.windowStore[(Long, String), Long]())

    val allTimesCount: ReadOnlyKeyValueStore[(Long, String), Long] = streamApp.store(parametersAllTimesViewCount)
    val allTimesCountPerCategory: ReadOnlyKeyValueStore[(Long, String), Long] = streamApp.store(parametersAllTimesViewCountPerCategory)
    val recentViewsCountPerCategory: ReadOnlyWindowStore[(Long, String), Long] = streamApp.store(parametersLastFiveMinutesCountPerCategory)

    val resAllTimes = allTimesCount.all().asScala.toList.find(res => res.key._1 == id)
    val resAllTimesPerCategory = allTimesCountPerCategory.all().asScala.toList.filter(res => res.key._1 == id).map(res => new TemporalStats(res.key._2, res.value))
    val resRecentViewsCountPerCategory = recentViewsCountPerCategory.fetchAll(from.toInstant, to.toInstant).asScala.toList.filter(res => res.key.key()._1 == id)
      .flatMap{ res =>
        val timestamp = res.key.window().startTime().atOffset(ZoneOffset.UTC)
        if (timestamp.isAfter(from) && timestamp.isBefore(to)){
          Some(new TemporalStats(res.key.key()._2, res.value))
        } else {
          None
        }
    }
    val movieView = new ViewsPerMovies(_id = resAllTimes.get.key._1, title = resAllTimes.get.key._2,
        total_view_count = resAllTimes.get.value,
        stats = new Stats(past = resAllTimesPerCategory, last_five_minutes = resRecentViewsCountPerCategory))

    List(movieView)
  }
}
object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    val api = new API(streams)
    val from = OffsetDateTime.now().minus(Duration.ofMinutes(5))
    val to = OffsetDateTime.now()
    concat(
      path("visits" / Segment) { period: String =>
        get {
          complete(
            List(VisitCountResponse("", 0))
          )
        }
      },
      path("latency" / "beginning") {
        get {
          complete(
            List(MeanLatencyForURLResponse("", 0))
          )
        }
      },
      path("movies" / LongNumber) { id: Long =>
        get {
          complete(
            api.viewsPerMovies(from, to, id)
          )
        }
      }
    )
  }
}
