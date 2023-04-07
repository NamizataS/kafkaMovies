package org.esgi.project.api

import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{MovieAverageScore, Stats, StatsDetails, ViewsMovieStats, ViewsPerMovies}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.AverageScoreForMovie

import scala.jdk.CollectionConverters._
import java.time.{OffsetDateTime, ZoneOffset}

class API(streamApp: KafkaStreams){
  private val parametersMovieTitles = StoreQueryParameters.fromNameAndType(StreamProcessing.movieTitlesStoreName, QueryableStoreTypes.keyValueStore[Long, String]())

  /***
   *
   * @param from
   * @param to
   * @param id
   * @return
   */
  def viewsPerMovies(from: OffsetDateTime, to: OffsetDateTime, id: Long): List[ViewsPerMovies] = {
    val categories : List[String] = List("start_only", "half", "full")

    val parametersAllTimesViewCount = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimeViewsCountStoreName, QueryableStoreTypes.keyValueStore[Long, Long]())
    val parametersAllTimesViewCountPerCategory = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimesViewsPerCategoryCountStoreName, QueryableStoreTypes.keyValueStore[(Long, String), Long]())
    val parametersLastFiveMinutesCountPerCategory = StoreQueryParameters.fromNameAndType(StreamProcessing.recentViewsPerCategoryCountStoreName, QueryableStoreTypes.windowStore[(Long, String), Long]())

    val allTimesCount: ReadOnlyKeyValueStore[Long, Long] = streamApp.store(parametersAllTimesViewCount)
    val allTimesCountPerCategory: ReadOnlyKeyValueStore[(Long, String), Long] = streamApp.store(parametersAllTimesViewCountPerCategory)
    val recentViewsCountPerCategory: ReadOnlyWindowStore[(Long, String), Long] = streamApp.store(parametersLastFiveMinutesCountPerCategory)
    val moviesTitles: ReadOnlyKeyValueStore[Long, String] = streamApp.store(parametersMovieTitles)

    val countAllTime: Long = Option(allTimesCount.get(id)).getOrElse(0L)
    val countAllTimePerCategory: Map[String, Long] = categories.map{cat =>
      val count: Long = Option(allTimesCountPerCategory.get((id, cat))).getOrElse(0L)
      (cat, count)
    }.toMap

    val availableInWindow = recentViewsCountPerCategory.fetchAll(from.toInstant, to.toInstant).asScala.toList
    val countRecentViewsPerCategory: Map[String, Long] = categories.map{cat =>
      val availableInWindowForIdAndCat = availableInWindow.filter(movie => movie.key.key() == (id, cat) && movie.key.window().startTime().atOffset(ZoneOffset.UTC).isAfter(from)
      && movie.key.window().startTime().atOffset(ZoneOffset.UTC).isBefore(to))
      val count:Long = availableInWindowForIdAndCat.headOption match {
        case Some(availableInWindow) => availableInWindow.value
        case None => 0L
      }
      (cat, count)
    }.toMap

    val movieTitle: String = {
      if(countAllTime > 0){
        moviesTitles.get(id)
      } else {
        ""
      }
    }
    val pastStats = new StatsDetails(start_only = countAllTimePerCategory("start_only"),
      half = countAllTimePerCategory("half"),
      full = countAllTimePerCategory("full"))

    val recentStats = new StatsDetails(start_only = countRecentViewsPerCategory("start_only"),
      half = countRecentViewsPerCategory("half"),
      full = countRecentViewsPerCategory("full"))

    val viewsPerMovie = new ViewsPerMovies(_id = id, title = movieTitle, total_view_count = countAllTime,
      stats = new Stats(past = pastStats, last_five_minutes = recentStats))

    List(viewsPerMovie)
  }

  /***
   *
   * @param best
   * @return
   */
  def tenBestOrWorseAverageScore(best: Boolean): List[MovieAverageScore] = {
    val parametersAllTimesAverageScore = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimesTenBestAverageScoreStoreName, QueryableStoreTypes.keyValueStore[(Long, String), AverageScoreForMovie]())

    val averageScoresAllTimes: ReadOnlyKeyValueStore[(Long, String), AverageScoreForMovie] = streamApp.store(parametersAllTimesAverageScore)
    val sortedList = if (best) {
      averageScoresAllTimes.all().asScala.toList.sortBy(movie => movie.value.averageScore)(Ordering[Double].reverse)
    } else {
      averageScoresAllTimes.all().asScala.toList.sortBy(movie => movie.value.averageScore)
    }
    val topTenMovies = sortedList.take(10)
    val resObject = topTenMovies.map { movie =>
      new MovieAverageScore(id = movie.key._1, title = movie.key._2, score = (movie.value.sum / movie.value.count))
    }
    resObject
  }

  /***
   *
   * @param best
   * @return
   */
  def tenBestOrWorseViews(best: Boolean): List[ViewsMovieStats] = {
    val parametersAllTimesViews = StoreQueryParameters.fromNameAndType(StreamProcessing.allTimeViewsCountStoreName, QueryableStoreTypes.keyValueStore[Long, Long]())
    val parametersMovieTitles = StoreQueryParameters.fromNameAndType(StreamProcessing.movieTitlesStoreName, QueryableStoreTypes.keyValueStore[Long, String]())

    val allTimesViews: ReadOnlyKeyValueStore[Long, Long] = streamApp.store(parametersAllTimesViews)
    val movieTitles: ReadOnlyKeyValueStore[Long, String] = streamApp.store(parametersMovieTitles)

    val sortedList = if (best) {
      allTimesViews.all().asScala.toList.sortBy(movie => movie.value)(Ordering[Long].reverse)
    } else {
      allTimesViews.all().asScala.toList.sortBy(movie => movie.value)
    }
    val topTenMovies = sortedList.take(10)
    val resObject = topTenMovies.map{ movie =>
      val movieTitle = movieTitles.get(movie.key)
      new ViewsMovieStats(id = movie.key, title = movieTitle, views = movie.value)
    }
    resObject
  }

}