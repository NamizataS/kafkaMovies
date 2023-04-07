package org.esgi.project.streaming.models

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Json, OFormat}

case class Like(id: Long, score: Double)
class LikeSerializer extends Serializer[Like] {
  override def serialize(topic: String, data: Like): Array[Byte] = {
    Json.toBytes(Json.toJson(data))
  }
}
class LikeDeserializer extends Deserializer[Like] {
  override def deserialize(topic: String, data: Array[Byte]): Like = {
    Json.fromJson[Like](Json.parse(data)).get
  }
}
object Like {
  implicit val format: OFormat[Like] = Json.format[Like]
}
