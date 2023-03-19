package streaming

import org.esgi.project.streaming.models.{Likes, Views}
import streaming.Tools.Models.GeneratedView
import org.apache.kafka.streams.test.TestRecord

import scala.annotation.tailrec
import scala.util.Random

object Tools {

  object Models {
    case class GeneratedView(view: Views, like: Likes)
  }

  object Utils {
    val viewsCategory: List[String] = List("start_only", "half", "full")
    val moviesTitles: List[String] = List("Becca est un clown", "Interstellar", "Alexandre est un couilloncadeau")

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
      GeneratedView(view = new Views(_id = id, title = movieTitle, viewsCategory = viewCategory),
        like = new Likes(_id = id, score = score))
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
    /***
     *
     * @param view
     */
    implicit class ViewToTestRecord(view: Views){
      def toTestRecord: TestRecord[Long, Views] = new TestRecord[Long, Views](view._id, view)
    }

    implicit class LikesToTestRecord(like: Likes){
      def toTestRecord: TestRecord[Long, Likes] = new TestRecord[Long, Likes](like._id, like)
    }
  }
}