package streaming

import org.esgi.project.streaming.models.{Like, View}
import streaming.Tools.Models.GeneratedView
import org.apache.kafka.streams.test.TestRecord

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.annotation.tailrec
import scala.util.Random

object Tools {


  object Models {
    case class GeneratedView(view: View, like: Like, recordTimestamp: Instant)
  }

  object Utils {
    val viewsCategory: List[String] = List("start_only", "half", "full")
    val moviesTitles: List[String] = List("Star Wars", "Interstellar", "Inception", "Shadow and bone")

    /** *
     * To generate a single view and like
     *
     * @return a GeneratedView with a view and a like event
     */
    def generateView: GeneratedView = {
      val id: Long = Random.nextLong(moviesTitles.length)
      val movieTitle: String = moviesTitles(id.toInt)
      val score: Double = Math.min(Random.nextDouble * 101, 100)
      val viewCategory = viewsCategory(Random.nextInt(viewsCategory.length))
      val recordTimestamp: Instant = OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofMinutes(Random.nextInt(15) + 1)).toInstant
      GeneratedView(view = new View(id = id, title = movieTitle, adult = false, view_category = viewCategory),
        like = new Like(id = id, score = score), recordTimestamp = recordTimestamp)
    }

    /** *
     * Generate a list of events
     *
     * @param countEvents : quantity of events to generate
     * @return a list with all the events generated
     */
    def generateEvents(countEvents: Long): List[GeneratedView] = {
      /** *
       * Generate a list of events using tail recursion
       *
       * @param countEvents : quantity of events to generate
       * @param acc         : number of events generated
       * @param res         : list of generated events
       * @return a list with all the events generated
       */
      @tailrec
      def insideGenerateEvents(countEvents: Long, acc: Long, res: List[GeneratedView]): List[GeneratedView] = {
        acc match {
          case _ if (acc == countEvents) => res
          case _ => insideGenerateEvents(countEvents, acc + 1, res :+ generateView)
        }
      }

      insideGenerateEvents(countEvents, 0, List())
    }
  }

  object Converters {
    implicit class ViewToTestRecord(view: View){
      def toTestRecord(recordTimestamp: Instant): TestRecord[String, View] = new TestRecord[String, View](view.id.toString, view, recordTimestamp)
    }

    implicit class LikesToTestRecord(like: Like){
      def toTestRecord(recordTimestamp: Instant): TestRecord[String, Like] = new TestRecord[String, Like](like.id.toString, like, recordTimestamp)
    }
  }
}
