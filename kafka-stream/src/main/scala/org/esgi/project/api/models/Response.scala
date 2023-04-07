package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class StatsDetails(start_only: Long, half: Long, full: Long)

case class Stats(
    past: StatsDetails,
    last_five_minutes: StatsDetails)
case class ViewsPerMovies(
    _id: Long,
    title: String,
    total_view_count: Long,
    stats: Stats)

case class MovieAverageScore(id: Long, title: String, score: Double)
case class ViewsMovieStats(id: Long, title: String, views: Long)

// implicits
object StatsDetails {
  implicit val format: OFormat[StatsDetails] = Json.format[StatsDetails]
}
object Stats {
  implicit val format: OFormat[Stats] = Json.format[Stats]
}

object ViewsPerMovies {
  implicit val format: OFormat[ViewsPerMovies] = Json.format[ViewsPerMovies]
}

object MovieAverageScore{
  implicit val format: OFormat[MovieAverageScore] = Json.format[MovieAverageScore]
}

object ViewsMovieStats{
  implicit val format: OFormat[ViewsMovieStats] = Json.format[ViewsMovieStats]
}