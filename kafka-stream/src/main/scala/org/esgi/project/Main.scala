package org.esgi.project


import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import com.fasterxml.jackson.databind.deser.std.NumberDeserializers.IntegerDeserializer
import com.typesafe.config.{Config, ConfigFactory}
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.common.serialization.{LongDeserializer, Serde, StringDeserializer}
import org.apache.kafka.streams.KafkaStreams
import org.esgi.project.api.WebServer
import org.esgi.project.streaming.StreamProcessing
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.streams.KafkaStreams.State
import org.esgi.project.streaming.models.{Like, LikeDeserializer, View}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.esgi.project.streaming.StreamProcessing.toSerde

import java.time.Duration
import java.util.{Collections, Properties}
import scala.concurrent.ExecutionContextExecutor

object Main {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    val streams: KafkaStreams = StreamProcessing.run()
    while (!List(State.RUNNING, State.ERROR).contains(streams.state())) {
      Thread.sleep(1000)
    }
    println("Server start")
    Http()
      .newServerAt("0.0.0.0", 8080)
      .bindFlow(WebServer.routes(streams))
    logger.info(s"App started on 8080")
  }
}
