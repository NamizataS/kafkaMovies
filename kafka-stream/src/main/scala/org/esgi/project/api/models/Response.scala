package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class TemporalStats(
    category: String,
    count: Long)

case class Stats(
    past: List[TemporalStats],
    last_five_minutes: List[TemporalStats])
case class ViewsPerMovies(
    _id: Long,
    title: String,
    total_view_count: Long,
    stats: Stats)

// implicits

object TemporalStats {
  implicit val format: OFormat[TemporalStats] = Json.format[TemporalStats]
}

object Stats {
  implicit val format: OFormat[Stats] = Json.format[Stats]
}

object ViewsPerMovies {
  implicit val format: OFormat[ViewsPerMovies] = Json.format[ViewsPerMovies]
}