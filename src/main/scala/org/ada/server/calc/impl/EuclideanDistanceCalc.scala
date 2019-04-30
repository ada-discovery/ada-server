package org.ada.server.calc.impl

object EuclideanDistanceCalc extends DistanceCalc[Option[Double], DistanceCalcTypePack[Option[Double]]] {

  override protected def dist(
    el1: Option[Double],
    el2: Option[Double]
  ) =
    (el1, el2).zipped.headOption.map { case (value1, value2) =>
      val diff = value1 - value2
      diff * diff
    }

  override protected def processSum(sum: Double) = Math.sqrt(sum)
}

object AllDefinedEuclideanDistanceCalc extends DistanceCalc[Double, DistanceCalcTypePack[Double]] {

  override protected def dist(
    value1: Double,
    value2: Double
  ) = {
    val diff = value1 - value2
    Some(diff * diff)
  }

  override protected def processSum(sum: Double) = Math.sqrt(sum)
}