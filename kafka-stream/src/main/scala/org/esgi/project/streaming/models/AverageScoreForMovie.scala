package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

/***
 *
 * @param sum
 * @param count
 * @param averageScore
 */
case class AverageScoreForMovie(sum: Double, count: Double, averageScore: Double){
  /***
   *
   * @param score
   * @return
   */
  def increment(score: Double): AverageScoreForMovie = this.copy(sum = this.sum + score, count = this.count + 1).computeMeanScore

  /***
   *
   * @return
   */
  def computeMeanScore: AverageScoreForMovie = this.copy(averageScore = this.sum / this.count)
}

object AverageScoreForMovie {
  implicit val format: OFormat[AverageScoreForMovie] = Json.format[AverageScoreForMovie]

  /***
   *
   * @return
   */
  def empty: AverageScoreForMovie = AverageScoreForMovie(0, 0, 0)
}
