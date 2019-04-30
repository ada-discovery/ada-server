package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Sink}
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import play.api.Logger

import scala.collection.mutable

trait DistanceCalcTypePack[T] extends CalculatorTypePack {
  type IN = Seq[T]
  type OUT = Seq[Seq[Double]]
  type INTER = Array[mutable.ArraySeq[Double]]
  type OPT = Unit
  type FLOW_OPT = Option[Int]
  type SINK_OPT = Unit
}

protected abstract class DistanceCalc[T, C <: DistanceCalcTypePack[T]] extends Calculator[C] with MatrixCalcHelper {

  private val logger = Logger

  protected def dist(el1: T, el2: T): Option[Double]

  protected def processSum(sum: Double): Double

  override def fun(o: Unit) = { values: Traversable[Seq[T]] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    def calc(index1: Int, index2: Int) = {
      // calculating distances and summing up
      val sum = values.foldLeft(0.0) { case (sum, els) =>
        dist(els(index1), els(index2)).map( distance =>
          sum + distance
        ).getOrElse(
          sum
        )
      }

      processSum(sum)
    }

    (0 until elementsCount).par.map { i =>
      (0 until elementsCount).par.map { j =>
        if (i != j) calc(i, j) else 0
      }.toList
    }.toList
  }

  override def flow(paralellism: Option[Int]) = {
    Flow[IN].fold[INTER](
      Array()
    ) {
      case (accumGlobal, featureValues) =>
        val n = featureValues.size

        // calculate the optimal computation start and end indices in the matrix
        val startEnds = calcStartEnds(n, paralellism)

        // if the accumulator is empty, initialized it with zero counts/sums
        val initializedAccumGlobal =
          if (accumGlobal.length == 0) {
            (for (i <- 0 to n - 1) yield mutable.ArraySeq(Seq.fill(i)(0d): _*)).toArray
          } else
            accumGlobal

        def calcAux(from: Int, to: Int) =
          for (i <- from to to) {
            val rowPSums = initializedAccumGlobal(i)
            val value1 = featureValues(i)
            for (j <- 0 to i - 1) {
              val value2 = featureValues(j)
              dist(value1, value2).foreach { distance =>
                rowPSums.update(j, rowPSums(j) + distance)
              }
            }
          }

        startEnds match {
          case Nil => calcAux(0, n - 1)
          case _ => startEnds.par.foreach((calcAux(_, _)).tupled)
        }
        initializedAccumGlobal
    }
  }

  override def postFlow(options: SINK_OPT) = { triangleResults: INTER =>
    val n = triangleResults.length
    logger.info("Generating a full matrix from the triangle sum results.")

    for (i <- 0 to n - 1) yield
      for (j <- 0 to n - 1) yield {
        if (i > j)
          processSum(triangleResults(i)(j))
        else if (i < j)
          processSum(triangleResults(j)(i))
        else
          0d
      }
  }
}