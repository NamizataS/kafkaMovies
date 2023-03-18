package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

/***
 *
 * @param sum
 * @param count
 * @param meanScore
 */
case class MeanScoreForMovie(sum: Long, count: Long, meanScore: Long){
  /***
   *
   * @param score
   * @return
   */
  def increment(score: Long): MeanScoreForMovie = this.copy(sum = this.sum + score, count = this.count + 1)

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
