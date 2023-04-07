package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

/***
 *
 * Case class to describe views distribution
 * @param start_only : Number of views with less than 10% of the movie watched
 * @param half : Number of views with less than 90% of the movie watched
 * @param full : Number of views with more than 90% of the movie watched
 */
case class StatsDetails(start_only: Long, half: Long, full: Long)

/***
 *
 * Case class to describe the needed distributions for the API
 * @param past : Distribution of views of all time
 * @param last_five_minutes : Distribution  of views in the last five miniutes
 */
case class Stats(
    past: StatsDetails,
    last_five_minutes: StatsDetails)

/***
 * Add description
 * @param _id : Movie id
 * @param title : Movie title
 * @param total_view_count : Total view count for the movie
 * @param stats : Distribution of views for the movie
 */
case class ViewsPerMovies(
    _id: Long,
    title: String,
    total_view_count: Long,
    stats: Stats)

/***
 * Add description
 * @param id : Movie id
 * @param title : Movie title
 * @param score : Movie average score
 */
case class MovieAverageScore(id: Long, title: String, score: Double)

/***
 * Add description
 * @param id : Movie id
 * @param title : Movie title
 * @param views : Number of views for the movie
 */
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