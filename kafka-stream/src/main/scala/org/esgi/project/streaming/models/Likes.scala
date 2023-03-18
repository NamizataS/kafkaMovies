package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Likes (_id: Int, score: Double)

object Likes {
  implicit val format: OFormat[Likes] = Json.format[Likes]
}
