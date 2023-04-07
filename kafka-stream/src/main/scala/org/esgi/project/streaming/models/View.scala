package org.esgi.project.streaming.models

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Json, OFormat}

case class View(id: Long, title: String, adult: Boolean, view_category: String)

class ViewSerializer extends Serializer[View] {
  override def serialize(topic: String, data: View): Array[Byte] = {
    Json.toBytes(Json.toJson(data))
  }
}

class ViewDeserializer extends Deserializer[View] {
  override def deserialize(topic: String, data: Array[Byte]): View = {
    Json.fromJson[View](Json.parse(data)).get
  }
}

object View {
  implicit val format: OFormat[View] = Json.format[View]
}
