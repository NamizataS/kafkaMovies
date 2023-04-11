package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Like(id: Long, score: Double)
object Like {
  implicit val format: OFormat[Like] = Json.format[Like]
}
