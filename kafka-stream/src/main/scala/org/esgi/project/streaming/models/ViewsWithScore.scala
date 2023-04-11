package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

/***
 * Class to join Views and Likes
 * @param _id : movie id
 * @param title : movie title
 * @param score : movie score
 */
case class ViewsWithScore(_id: Long, title: String, score: Double)

object ViewsWithScore {
  implicit val format: OFormat[ViewsWithScore] = Json.format[ViewsWithScore]
}
