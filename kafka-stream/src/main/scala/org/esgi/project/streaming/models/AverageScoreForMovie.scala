package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

/***
 * Case class to compute the average score of a movie
 * @param sum : sum of values
 * @param count : number of values
 * @param averageScore : current average
 */
case class AverageScoreForMovie(sum: Double, count: Double, averageScore: Double){
  /***
   * Update the average score when a new score is found
   * @param score : score we need to add
   * @return : AverageScoreForMovie updated with new score
   */
  def increment(score: Double): AverageScoreForMovie = this.copy(sum = this.sum + score, count = this.count + 1).computeAverageScore

  /***
   * Compute the average score
   * @return : AverageScoreForMovie with average computed and updated
   */
  private def computeAverageScore: AverageScoreForMovie = this.copy(averageScore = this.sum / this.count)
}

object AverageScoreForMovie {
  implicit val format: OFormat[AverageScoreForMovie] = Json.format[AverageScoreForMovie]

  /***
   *
   * @return : An empty AverageScoreForMovie
   */
  def empty: AverageScoreForMovie = AverageScoreForMovie(0, 0, 0)
}
