package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

/***
 *
 * @param sum
 * @param count
 * @param meanScore
 */
case class MeanScoreForMovie(sum: Double, count: Double, meanScore: Double){
  /***
   *
   * @param score
   * @return
   */
  def increment(score: Double): MeanScoreForMovie = this.copy(sum = this.sum + score, count = this.count + 1).computeMeanScore

  /***
   *
   * @return
   */
  def computeMeanScore: MeanScoreForMovie = this.copy(meanScore = this.sum / this.count)
}

object MeanScoreForMovie {
  implicit val format: OFormat[MeanScoreForMovie] = Json.format[MeanScoreForMovie]

  /***
   *
   * @return
   */
  def empty: MeanScoreForMovie = MeanScoreForMovie(0, 0, 0)
}
