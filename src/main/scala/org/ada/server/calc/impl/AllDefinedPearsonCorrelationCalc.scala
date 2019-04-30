package org.ada.server.calc.impl

import akka.stream.scaladsl.{Flow, Sink}
import org.ada.server.calc.{Calculator, CalculatorTypePack}
import org.incal.core.util.GrouppedVariousSize
import play.api.Logger

import scala.collection.mutable

trait AllDefinedPearsonCorrelationCalcTypePack extends CalculatorTypePack {
  type IN = Seq[Double]
  type OUT = Seq[Seq[Option[Double]]]
  type INTER = PersonIterativeAccumGlobalArray
  type OPT = Unit
  type FLOW_OPT = Option[Int]
  type SINK_OPT = Option[Int]
}

object AllDefinedPearsonCorrelationCalc extends Calculator[AllDefinedPearsonCorrelationCalcTypePack] with MatrixCalcHelper {

  private val logger = Logger

  override def fun(o: Unit) = { values: Traversable[IN] =>
    val elementsCount = if (values.nonEmpty) values.head.size else 0

    def calc(index1: Int, index2: Int) = {
      val els = (
        values.map(_ (index1)).toSeq,
        values.map(_ (index2)).toSeq
      ).zipped

      PearsonCorrelationCalc.calcForPair(els)
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

  override def flow(parallelism: Option[Int]) = {
    Flow[IN].fold[PersonIterativeAccumGlobalArray](
      PersonIterativeAccumGlobalArray(Nil, Array(), 0)
    ) {
      case (accumGlobal, featureValues) =>
        val n = featureValues.size

        // calculate the optimal computation start and end indices in the matrix
        val startEnds = calcStartEnds(n, parallelism)

        // if the accumulator is empty, initialized it with zero counts/sums
        val initializedAccumGlobal: PersonIterativeAccumGlobalArray =
          if (accumGlobal.sumSqSums.isEmpty)
            PersonIterativeAccumGlobalArray(
              sumSqSums = for (i <- 0 to n - 1) yield (0d, 0d),
              pSums = (for (i <- 0 to n - 1) yield mutable.ArraySeq(Seq.fill(i)(0d): _*)).toArray,
              count = 0
            )
          else
            accumGlobal

        val newSumSqSums = (initializedAccumGlobal.sumSqSums, featureValues).zipped.map {
          case ((sum, sqSum), value) => (sum + value, sqSum + value * value)
        }

        val pSums = initializedAccumGlobal.pSums

        def calcAux(from: Int, to: Int) =
          for (i <- from to to) {
            val rowPSums = pSums(i)
            val value1 = featureValues(i)
            for (j <- 0 to i - 1) {
              val value2 = featureValues(j)
              rowPSums.update(j, rowPSums(j) + value1 * value2)
            }
          }

        startEnds match {
          case Nil => calcAux(0, n - 1)
          case _ => startEnds.par.foreach((calcAux(_, _)).tupled)
        }
        PersonIterativeAccumGlobalArray(newSumSqSums, pSums, initializedAccumGlobal.count + 1)
    }
  }

  @Deprecated
  private def flowOld(
    n: Int,
    parallelGroupSizes: Seq[Int]
  ) =
    Flow[Seq[Double]].fold[PersonIterativeAccumGlobal](
      PersonIterativeAccumGlobal(
        sumSqSums = for (i <- 0 to n - 1) yield (0d, 0d),
        pSums = for (i <- 0 to n - 1) yield Seq.fill(i)(0d),
        count = 0
      )
    ) {
      case (accumGlobal, featureValues) =>
        val newSumSqSums = (accumGlobal.sumSqSums, featureValues).zipped.map { case ((sum, sqSum), value) =>
          (sum + value, sqSum + value * value)
        }

        def calcAux(pSumValuePairs: Traversable[(Seq[Double], Double)]) =
          pSumValuePairs.map { case (pSums, value1) =>
            (pSums, featureValues).zipped.map { case (pSum, value2) =>
              pSum + value1 * value2
            }
          }

        val pSumValuePairs = (accumGlobal.pSums, featureValues).zipped.toTraversable

        val newPSums = parallelGroupSizes match {
          case Nil => calcAux(pSumValuePairs)
          case _ => pSumValuePairs.grouped(parallelGroupSizes).toArray.par.flatMap(calcAux).arrayseq
        }
        PersonIterativeAccumGlobal(newSumSqSums, newPSums.toSeq, accumGlobal.count + 1)
    }

  override def postFlow(parallelism: Option[Int]) = { globalAccum: INTER =>
    val accums = globalAccumToAccums(globalAccum)
    PearsonCorrelationCalc.postFlow(parallelism)(accums)
  }

  private def globalAccumToAccums(
    globalAccum: PersonIterativeAccumGlobalArray
  ): Seq[Seq[PersonIterativeAccum]] = {
    logger.info("Converting the global streamed accumulator to the partial ones.")
    (globalAccum.pSums, globalAccum.sumSqSums).zipped.map { case (rowPSums, (sum1, sqSum1)) =>
      (rowPSums, globalAccum.sumSqSums).zipped.map { case (pSum, (sum2, sqSum2)) =>
        PersonIterativeAccum(sum1, sum2, sqSum1, sqSum2, pSum, globalAccum.count)
      }
    }
  }

  @Deprecated
  private def globalAccumToAccumsOld(
    globalAccum: PersonIterativeAccumGlobal
  ): Seq[Seq[PersonIterativeAccum]] = {
    logger.info("Converting the global streamed accumulator to the partial ones.")
    (globalAccum.pSums, globalAccum.sumSqSums).zipped.map { case (rowPSums, (sum1, sqSum1)) =>
      (rowPSums, globalAccum.sumSqSums).zipped.map { case (pSum, (sum2, sqSum2)) =>
        PersonIterativeAccum(sum1, sum2, sqSum1, sqSum2, pSum, globalAccum.count)
      }
    }
  }
}

case class PersonIterativeAccumGlobalArray(
  sumSqSums: Seq[(Double, Double)],
  pSums: Array[mutable.ArraySeq[Double]],
  count: Int
)

@Deprecated
case class PersonIterativeAccumGlobal(
  sumSqSums: Seq[(Double, Double)],
  pSums: Seq[Seq[Double]],
  count: Int
)