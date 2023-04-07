package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.esgi.project.api.models.VisitCountResponse


import java.time.{Duration, OffsetDateTime, ZoneOffset}

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
            List(None)
          )
        }
      },
      path("movies" / LongNumber) { id: Long =>
        get {
          complete(
            api.viewsPerMovies(from, to, id)
          )
        }
      },
      path("stats" / "ten" / "best" / "score"){
        get {
          complete(
            api.tenBestOrWorseScore(true)
          )
        }
      },
      path("stats" / "ten" / "best" / "views"){
        get{
          complete(
            api.tenBestOrWorseViews(true)
          )
        }
      },
      path("stats" / "ten" / "worst" / "score"){
        get{
          complete(
            api.tenBestOrWorseScore(false)
          )
        }
      },
      path("stats" / "ten" / "worst" / "views"){
        get{
          complete(
            api.tenBestOrWorseViews(false)
          )
        }
      }
    )
  }
}
