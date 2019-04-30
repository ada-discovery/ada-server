package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Sink}
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.util.GrouppedVariousSize
import play.api.Logger

import scala.collection.mutable

trait MatthewsBinaryClassCorrelationCalcTypePack extends CalculatorTypePack {
  type IN = Seq[Option[Boolean]]
  type OUT = Seq[Seq[Option[Double]]]
  type INTER = Seq[Seq[ClassMatchCounts]]
  type OPT = Unit
  type FLOW_OPT = Option[Int]
  type SINK_OPT = Option[Int]
}

object MatthewsBinaryClassCorrelationCalc extends Calculator[MatthewsBinaryClassCorrelationCalcTypePack] with MatrixCalcHelper {

  private val logger = Logger

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

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
        if (i != j) calc(i, j) else Some(1d)
      }.toList
    }.toList
  }

  protected[impl] def calcForPair(
    els: Traversable[(Boolean, Boolean)]
  ): Option[Double] =
    if (els.nonEmpty) {
      // calc counts
      val counts = els.foldLeft(ClassMatchCounts())(updateCounts)

      // return the MCC
      calcMCC(counts)
    } else
      None


  private def updateCounts(
    counts: ClassMatchCounts,
    pair: (Boolean, Boolean)
  ): ClassMatchCounts =
    pair match {
      case (true, true) => counts.incTP
      case (false, false) => counts.incTN
      case (true, false) => counts.incFN
      case (false, true) => counts.incFP
    }

  private def calcMCC(counts: ClassMatchCounts) = {
    val numerator = counts.tp * counts.tn - counts.fp * counts.fn

    if (numerator == 0)
      Some(0d)
    else {
      val denominator = Math.sqrt(
        (counts.tp + counts.fp) * (counts.tp + counts.fn) * (counts.tn + counts.fp) * (counts.tn + counts.fn)
      )

      if (denominator == 0) None else Some(numerator / denominator)
    }
  }

  override def flow(parallelism: Option[Int]) = {
    Flow[IN].fold[INTER](Nil) {
      case (interCounts, featureValues) =>
        val n = featureValues.size

        // if accumulators are empty, initialized them with zero counts
        val initializedCounts =
          if (interCounts.isEmpty)
            for (i <- 0 to n - 1) yield Seq.fill(i)(ClassMatchCounts())
          else
            interCounts

        // calculate the optimal parallel computation group sizes
        val groupSizes = calcGroupSizes(n, parallelism)

        // helper function to calculate correlations for a slice of matrix
        def calcAux(
          countFeatureValuePairs: Seq[(Seq[ClassMatchCounts], Option[Boolean])]
        ) =
          countFeatureValuePairs.map { case (rowCounts, value1) =>
            rowCounts.zip(featureValues).map { case (counts, value2) =>
              if (value1.isDefined && value2.isDefined)
                updateCounts(counts, (value1.get, value2.get))
              else
                counts
            }
          }

        val countFeatureValuePairs = initializedCounts.zip(featureValues)

        groupSizes match {
          case Nil => calcAux(countFeatureValuePairs)
          case _ => countFeatureValuePairs.grouped(groupSizes).toSeq.par.flatMap(calcAux).toList
        }
    }
  }

  override def postFlow(parallelism: Option[Int]) = { accums: INTER =>
    logger.info("Creating MCC correlations from the streamed accumulators.")
    val n = accums.size

    // calc optimal parallel computation group sizes
    val groupSizes = calcGroupSizes(n, parallelism)

    def calcAux(counts: Seq[Seq[ClassMatchCounts]]) = counts.map(_.map(calcMCC))

    val triangleResults = groupSizes match {
      case Nil => calcAux(accums)
      case _ => accums.grouped(groupSizes).toArray.par.flatMap(calcAux).arrayseq
    }

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
}

case class ClassMatchCounts(
  tp: Long = 0,
  tn: Long = 0,
  fp: Long = 0,
  fn: Long = 0
) {
  def incTP = copy(tp = tp + 1)
  def incTN = copy(tn = tn + 1)
  def incFP = copy(fp = fp + 1)
  def incFN = copy(fn = fn + 1)
}