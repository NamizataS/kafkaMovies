package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams


object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    val api = new API(streams)
    concat(
      path("movies" / LongNumber) { id: Long =>
        get {
          complete(
            api.viewsPerMovies(id)
          )
        }
      },
      path("stats" / "ten" / "best" / "score"){
        get {
          complete(
            api.tenBestOrWorseAverageScore(true)
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
            api.tenBestOrWorseAverageScore(false)
          )
        }
      },
      path("stats" / "ten" / "worst" / "views"){
        get{
          complete(
            api.tenBestOrWorseViews(false)
          )
        }
      },
      path("movies" / "list" / IntNumber) { count: Int =>
        get {
          complete(
            api.moviesAvailable(count)
          )
        }
      }
    )
  }
}
