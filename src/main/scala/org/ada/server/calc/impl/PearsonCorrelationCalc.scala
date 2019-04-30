package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Sink}
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.util.GrouppedVariousSize
import play.api.Logger

import scala.collection.mutable

trait PearsonCorrelationCalcTypePack extends CalculatorTypePack {
  type IN = Seq[Option[Double]]
  type OUT = Seq[Seq[Option[Double]]]
  type INTER = Seq[Seq[PersonIterativeAccum]]
  type OPT = Unit
  type FLOW_OPT = Option[Int]
  type SINK_OPT = Option[Int]
}

object PearsonCorrelationCalc extends Calculator[PearsonCorrelationCalcTypePack] with MatrixCalcHelper {

  private val logger = Logger

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    // aux function to calculate correlations for the columns at given indeces
    def calc(index1: Int, index2: Int) = {
      val els = (
        values.map(_ (index1)).toSeq,
        values.map(_ (index2)).toSeq
      ).zipped.flatMap {
        case (el1, el2) => (el1, el2).zipped.headOption
      }

      calcForPair(els)
    }

    (0 until elementsCount).par.map { i =>
      (0 until elementsCount).par.map { j =>
        if (i != j)
          calc(i, j)
        else
          Some(1d)
      }.toList
    }.toList
  }

  protected[impl] def calcForPair(
    els: Traversable[(Double, Double)]
  ): Option[Double] = {
    if (els.nonEmpty) {
      val length = els.size

      val mean1 = els.map(_._1).sum / length
      val mean2 = els.map(_._2).sum / length

      // sum up the squares
      val mean1Sq = els.map(_._1).foldLeft(0.0)(_ + Math.pow(_, 2)) / length
      val mean2Sq = els.map(_._2).foldLeft(0.0)(_ + Math.pow(_, 2)) / length

      // sum up the products
      val pMean = els.foldLeft(0.0) { case (accum, pair) => accum + pair._1 * pair._2 } / length

      // calculate the pearson score
      val numerator = pMean - mean1 * mean2

      val denominator = Math.sqrt(
        (mean1Sq - Math.pow(mean1, 2)) * (mean2Sq - Math.pow(mean2, 2))
      )
      if (denominator == 0)
        None
      else
        Some(numerator / denominator)
    } else
      None
  }

  override def flow(parallelism: Option[Int]) = {
    Flow[IN].fold[INTER](Nil) {
      case (accums, featureValues) =>
        val n = featureValues.size

        // if accumulators are empty, initialized them with zero counts/sums
        val initializedAccums =
          if (accums.isEmpty)
            for (i <- 0 to n - 1) yield Seq.fill(i)(PersonIterativeAccum(0, 0, 0, 0, 0, 0))
          else
            accums

        // calculate the optimal parallel computation group sizes
        val groupSizes = calcGroupSizes(n, parallelism)

        // helper function to calculate correlations for a slice of matrix
        def calcAux(accumFeatureValuePairs: Seq[(Seq[PersonIterativeAccum], Option[Double])]) =
          accumFeatureValuePairs.map { case (rowAccums, value1) =>
            rowAccums.zip(featureValues).map { case (accum, value2) =>
              if (value1.isDefined && value2.isDefined) {
                PersonIterativeAccum(
                  accum.sum1 + value1.get,
                  accum.sum2 + value2.get,
                  accum.sqSum1 + value1.get * value1.get,
                  accum.sqSum2 + value2.get * value2.get,
                  accum.pSum + value1.get * value2.get,
                  accum.count + 1
                )
              } else
                accum
            }
          }

        val accumFeatureValuePairs = initializedAccums.zip(featureValues)

        groupSizes match {
          case Nil => calcAux(accumFeatureValuePairs)
          case _ => accumFeatureValuePairs.grouped(groupSizes).toSeq.par.flatMap(calcAux).toList
        }
    }
  }

  @Deprecated
  // seems slower than @see pearsonCorrelationSink and could be removed
  private def flowMutable(
    n: Int,
    parallelGroupSizes: Seq[Int]
  ) = {
    val starts = parallelGroupSizes.scanLeft(0){_+_}
    val startEnds = parallelGroupSizes.zip(starts).map{ case (size, start) => (start, Math.min(start + size, n) - 1)}

    Flow[Seq[Option[Double]]].fold[Seq[mutable.ArraySeq[PersonIterativeAccum]]](
      for (i <- 0 to n - 1) yield mutable.ArraySeq(Seq.fill(i)(PersonIterativeAccum(0, 0, 0, 0, 0, 0)): _*)
    ) {
      case (accums, featureValues) =>

        def calcAux(from: Int, to: Int) =
          for (i <- from to to) {
            val rowAccums = accums(i)
            featureValues(i).foreach(value1 =>
              for (j <- 0 to i - 1) {
                featureValues(j).foreach { value2 =>
                  val accum = rowAccums(j)
                  val newAccum = PersonIterativeAccum(
                    accum.sum1 + value1,
                    accum.sum2 + value2,
                    accum.sqSum1 + value1 * value1,
                    accum.sqSum2 + value2 * value2,
                    accum.pSum + value1 * value2,
                    accum.count + 1
                  )
                  rowAccums.update(j, newAccum)
                }
              }
            )
          }

        startEnds match {
          case Nil => calcAux(0, n - 1)
          case _ => startEnds.par.foreach((calcAux(_, _)).tupled)
        }
        accums
    }
  }

  override def postFlow(parallelism: Option[Int]) = { accums: INTER =>
    logger.info("Creating correlations from the streamed accumulators.")
    val n = accums.size

    // calc optimal parallel computation group sizes
    val groupSizes = calcGroupSizes(n, parallelism)

    def calcAux(accums: Seq[Seq[PersonIterativeAccum]]) = accums.map(_.map(accumToCorrelation))

    val triangleResults = groupSizes match {
      case Nil => calcAux(accums)
      case _ => accums.grouped(groupSizes).toArray.par.flatMap(calcAux).arrayseq
    }

    logger.info("Triangle results finished. Generating a full matrix.")

    for (i <- 0 to n - 1) yield
      for (j <- 0 to n - 1) yield {
        if (i > j)
          triangleResults(i)(j)
        else if (i < j)
          triangleResults(j)(i)
        else
          Some(1d)
      }
  }

  private def accumToCorrelation(accum: PersonIterativeAccum): Option[Double] = {
    val length = accum.count

    if (length < 2) {
      None
    } else {
      val mean1 = accum.sum1 / length
      val mean2 = accum.sum2 / length

      // sum up the squares
      val mean1Sq = accum.sqSum1 / length
      val mean2Sq = accum.sqSum2 / length

      // sum up the products
      val pMean = accum.pSum / length

      // calculate the pearson score
      val numerator = pMean - mean1 * mean2

      val denominator = Math.sqrt((mean1Sq - mean1 * mean1) * (mean2Sq - mean2 * mean2))

      if (denominator == 0)
        None
      else if (denominator.isNaN || denominator.isInfinity) {
        logger.error(s"Got not-a-number denominator during a correlation calculation.")
        None
      } else if (numerator.isNaN || numerator.isInfinity) {
        logger.error(s"Got not-a-number numerator during a correlation calculation.")
        None
      } else
        Some(numerator/ denominator)
    }
  }
}

case class PersonIterativeAccum(
  sum1: Double,
  sum2: Double,
  sqSum1: Double,
  sqSum2: Double,
  pSum: Double,
  count: Int
)