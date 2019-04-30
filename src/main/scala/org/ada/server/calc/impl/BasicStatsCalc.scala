package org.ada.server.calc.impl

import akka.stream.scaladsl.Flow
import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}

trait BasicStatsCalcTypePack extends NoOptionsCalculatorTypePack{
  type IN = Option[Double]
  type OUT = Option[BasicStatsResult]
  type INTER = BasicStatsAccum
}

object BasicStatsCalc extends Calculator[BasicStatsCalcTypePack] {

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val flattenedValues = values.flatten
    if (flattenedValues.nonEmpty) {
      val count = flattenedValues.size

      val min = flattenedValues.min
      val max = flattenedValues.max
      val sum = flattenedValues.sum
      val mean = sum / count

      val sqSum = flattenedValues.foldLeft(0.0)((accum, value) => accum + value * value)
      val variance = sqSum / count - mean * mean
      val sVariance = sampleVariance(variance, count)

      Some(BasicStatsResult(min, max, mean, sum, sqSum, variance, Math.sqrt(variance), sVariance, Math.sqrt(sVariance), count, values.size - count))
    } else
      None
  }

  override def flow(o: Unit) =
    Flow[IN].fold[INTER](
      BasicStatsAccum(Double.MaxValue, Double.MinValue, 0, 0, 0, 0)
    ) {
      case (accum, value) => updateAccum(accum, value)
    }

  override def postFlow(o: Unit) = { accum: INTER =>
    if (accum.count > 0) {
      val mean = accum.sum / accum.count

      val variance = (accum.sqSum / accum.count) - mean * mean
      val sVariance = sampleVariance(variance, accum.count)

      Some(BasicStatsResult(
        accum.min, accum.max, mean, accum.sum, accum.sqSum, variance, Math.sqrt(variance), sVariance, Math.sqrt(sVariance), accum.count, accum.undefinedCount
      ))
    } else
      None
  }

  private def sampleVariance(variance: Double, count: Int) =
    if (count > 1) variance * count / (count - 1) else variance

  private[impl] def updateAccum(
    accum: BasicStatsAccum,
    value: Option[Double]
  ) =
    value match {
      case Some(value) =>
        BasicStatsAccum(
          Math.min(accum.min, value),
          Math.max(accum.max, value),
          accum.sum + value,
          accum.sqSum + value * value,
          accum.count + 1,
          accum.undefinedCount
        )

      case None => accum.copy(undefinedCount = accum.undefinedCount + 1)
    }
}

case class BasicStatsResult(
  min: Double,
  max: Double,
  mean: Double,
  sum: Double,
  sqSum: Double,
  variance: Double,
  standardDeviation: Double,
  sampleVariance: Double,
  sampleStandardDeviation: Double,
  definedCount: Int,
  undefinedCount: Int
)

case class BasicStatsAccum(
  min: Double,
  max: Double,
  sum: Double,
  sqSum: Double,
  count: Int,
  undefinedCount: Int
)