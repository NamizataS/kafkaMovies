package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewsWithScore(_id: Long, title: String, viewCategory: String, score: Double)

object ViewsWithScore {
  implicit val format: OFormat[ViewsWithScore] = Json.format[ViewsWithScore]
}
