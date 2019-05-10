package org.ada.server.runnables.core

import java.{util => ju}

import org.ada.server.calc.impl.{PersonIterativeAccumGlobal, PersonIterativeAccumGlobalArray}
import org.incal.core.util.GrouppedVariousSize

import scala.collection.mutable
import scala.collection.parallel.mutable.ParArray
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object CheckCorrelationAcumCalc extends App {

  private def calcPearsonCorrelation1(
    parallelGroupSizes: Seq[Int],
    accumGlobal: PersonIterativeAccumGlobal,
    featureValues: Seq[Double]
  ) = {
    val newSumSqSums = accumGlobal.sumSqSums.zip(featureValues).map { case ((sum, sqSum), value) =>
      (sum + value, sqSum + value * value)
    }

    def calcAux(pSumValuePairs: Seq[(Seq[Double], Double)]) =
      pSumValuePairs.map { case (pSums, value1) =>
        pSums.zip(featureValues).map { case (pSum, value2) =>
          pSum + value1 * value2
        }
      }

    val pSumValuePairs = accumGlobal.pSums.zip(featureValues)

    val newPSums = parallelGroupSizes match {
      case Nil => calcAux(pSumValuePairs)
      case _ => pSumValuePairs.grouped(parallelGroupSizes).toSeq.par.flatMap(calcAux).toList
    }
    PersonIterativeAccumGlobal(newSumSqSums, newPSums, accumGlobal.count + 1)
  }

  private def calcPearsonCorrelation2(
    parallelGroupSizes: Seq[Int],
    accumGlobal: PersonIterativeAccumGlobal,
    featureValues: Seq[Double]
  ) = {
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
      case Nil => calcAux(pSumValuePairs).toSeq
      case _ => pSumValuePairs.grouped(parallelGroupSizes).toArray.par.flatMap(calcAux).arrayseq
    }
    PersonIterativeAccumGlobal(newSumSqSums, newPSums, accumGlobal.count + 1)
  }

  private def calcAuxParallel(
    featureValues: Seq[Double])(
    pSums: Seq[Seq[Double]],
    startIndex: Int
  ): (Seq[Seq[Double]], Int) = {
    val results = pSums.zipWithIndex.map { case (pSums, index) =>
      val value1 = featureValues(startIndex + index)
      (pSums, featureValues).zipped.map { case (pSum, value2) =>
        pSum + value1 * value2
      }
    }
    (results, startIndex)
  }

  private def calcPearsonCorrelation3(
    accumGlobal: PersonIterativeAccumGlobalParallel,
    featureValues: Seq[Double]
  ) = {
    val newSumSqSums = (accumGlobal.sumSqSums, featureValues).zipped.map { case ((sum, sqSum), value) =>
      (sum + value, sqSum + value * value)
    }

    val calcAux = (calcAuxParallel(featureValues)(_,_)).tupled
    val newPSums = accumGlobal.pSumsWithIndeces.map(calcAux)
    PersonIterativeAccumGlobalParallel(newSumSqSums, newPSums, accumGlobal.count + 1)
  }

  private def calcAuxParallelFuture(
    featureValues: Seq[Double])(
    pSumsWithStartIndex: Future[(Seq[Seq[Double]], Int)]
  ): Future[(Seq[Seq[Double]], Int)] =
    for {
      (pSums, startIndex) <- pSumsWithStartIndex
    } yield {
      val results = pSums.zipWithIndex.map { case (pSums, index) =>
        val value1 = featureValues(startIndex + index)
        (pSums, featureValues).zipped.map { case (pSum, value2) =>
          pSum + value1 * value2
        }
      }
      (results, startIndex)
    }

  private def calcPearsonCorrelation4(
    accumGlobal: PersonIterativeAccumGlobalFuture,
    featureValues: Seq[Double]
  ) = {
    val newSumSqSums = (accumGlobal.sumSqSums, featureValues).zipped.map { case ((sum, sqSum), value) =>
      (sum + value, sqSum + value * value)
    }

    val newPSums = accumGlobal.pSumsWithIndeces.map(calcAuxParallelFuture(featureValues))
    PersonIterativeAccumGlobalFuture(newSumSqSums, newPSums, accumGlobal.count + 1)
  }

  private def calcPearsonCorrelation5(
    parallelGroupSizes: Seq[Int],
    accumGlobal: PersonIterativeAccumGlobalArray,
    featureValues: Seq[Double]
  ) = {
    val newSumSqSums = (accumGlobal.sumSqSums, featureValues).zipped.map { case ((sum, sqSum), value) =>
      (sum + value, sqSum + value * value)
    }

    val n = featureValues.size

    val pSums = accumGlobal.pSums

    def calcAux(from: Int,  to: Int) =
      for (i <- from to to) {
        val rowPSums = pSums(i)
        val value1 = featureValues(i)
        for (j <- 0 to i - 1) {
          val value2 = featureValues(j)
          rowPSums.update(j, rowPSums(j) + value1 * value2)
        }
      }

    parallelGroupSizes match {
      case Nil => calcAux(0, n-1)
      case _ => {
        val starts = parallelGroupSizes.scanLeft(0){_+_}
        parallelGroupSizes.zip(starts).par.foreach{ case (size, start) =>
          calcAux(start, Math.min(start + size, n) - 1)
        }
      }
    }
    PersonIterativeAccumGlobalArray(newSumSqSums, pSums, accumGlobal.count + 1)
  }

  private def calcGroupSizes(n: Int, parallelism: Option[Int]) =
    parallelism.map { groupNumber =>
      val initGroupSize = n / Math.sqrt(groupNumber)

      val groupSizes = (1 to groupNumber).map { i =>
        val doubleGroupSize = (Math.sqrt(i) - Math.sqrt(i - 1)) * initGroupSize
        Math.round(doubleGroupSize).toInt
      }.filter(_ > 0)

      val sum = groupSizes.sum
      val fixedGroupSizes = if (sum < n) {
        if (groupSizes.size > 1)
          groupSizes.take(groupSizes.size - 1) :+ (groupSizes.last + (n - sum))
        else if (groupSizes.size == 1)
          Seq(groupSizes.head + (n - sum))
        else
          Seq(n)
      } else
        groupSizes

      fixedGroupSizes
    }.getOrElse(
      Nil
    )

  private val n = 5600
  private val parallelism = 4
  private val repetitions = 20

  private val groupSizes = calcGroupSizes(n, Some(parallelism))

  private val initAccumGlobal = PersonIterativeAccumGlobal(
    sumSqSums = for (i <- 0 to n - 1) yield (Random.nextDouble, Random.nextDouble),
    pSums = for (i <- 0 to n - 1) yield for (j <- 0 to i - 1) yield Random.nextDouble,
    count = Random.nextInt()
  )

  private def convertAccumGlobalToParallel(
    accumGlobal: PersonIterativeAccumGlobal,
    parallelGroupSizes: Seq[Int]
  ) = {
    val groupedPSums = accumGlobal.pSums.grouped(parallelGroupSizes).toArray
    val sizes = groupedPSums.map(_.size).toList
    val starts = sizes.scanLeft(0){_+_}

    println("Sizes  : " + sizes.mkString(","))
    println("Starts : " + starts.mkString(","))

    PersonIterativeAccumGlobalParallel(
      accumGlobal.sumSqSums,
      groupedPSums.zip(starts).par,
      accumGlobal.count
    )
  }

  private def convertAccumGlobalToFuture(
    accumGlobal: PersonIterativeAccumGlobal,
    parallelGroupSizes: Seq[Int]
  ) = {
    val groupedPSums = accumGlobal.pSums.grouped(parallelGroupSizes).toArray
    val sizes = groupedPSums.map(_.size).toList
    val starts = sizes.scanLeft(0){_+_}

    println("Sizes  : " + sizes.mkString(","))
    println("Starts : " + starts.mkString(","))

    PersonIterativeAccumGlobalFuture(
      accumGlobal.sumSqSums,
      groupedPSums.zip(starts).map(Future(_)),
      accumGlobal.count
    )
  }

  private def convertAccumGlobalToArrayType(
    accumGlobal: PersonIterativeAccumGlobal
  ) = {
    val arrayPSums = accumGlobal.pSums.map(x => mutable.ArraySeq(x:_*)).toArray

    PersonIterativeAccumGlobalArray(
      accumGlobal.sumSqSums,
      arrayPSums,
      accumGlobal.count
    )
  }

  private def calcAux1(accumGlobal: PersonIterativeAccumGlobal): PersonIterativeAccumGlobal = {
    val featureValues =
      for (i <- 0 to n - 1) yield Random.nextDouble()

    calcPearsonCorrelation1(groupSizes, accumGlobal, featureValues)
  }

  private def calcAux2(accumGlobal: PersonIterativeAccumGlobal): PersonIterativeAccumGlobal = {
    val featureValues =
      for (i <- 0 to n - 1) yield Random.nextDouble()

    calcPearsonCorrelation2(groupSizes, accumGlobal, featureValues)
  }

  private def calcAux3(accumGlobal: PersonIterativeAccumGlobalParallel): PersonIterativeAccumGlobalParallel = {
    val featureValues =
      for (i <- 0 to n - 1) yield Random.nextDouble()

    calcPearsonCorrelation3(accumGlobal, featureValues) // mutable.ArraySeq.apply(featureValues :_*))
  }

  private def calcAux4(accumGlobal: PersonIterativeAccumGlobalFuture): PersonIterativeAccumGlobalFuture = {
    val featureValues =
      for (i <- 0 to n - 1) yield Random.nextDouble()

    calcPearsonCorrelation4(accumGlobal, featureValues) // mutable.ArraySeq.apply(featureValues :_*))
  }

  private def calcAux5(accumGlobal: PersonIterativeAccumGlobalArray): PersonIterativeAccumGlobalArray = {
    val featureValues =
      for (i <- 0 to n - 1) yield Random.nextDouble()

    calcPearsonCorrelation5(groupSizes, accumGlobal, featureValues)
  }

  private val startAccumGlobal = initAccumGlobal

  val start1 = new ju.Date
  (1 to repetitions).foldLeft(startAccumGlobal) { case (acum, _) => calcAux1(acum)}
  println(s"Finished in ${(new ju.Date().getTime - start1.getTime) / repetitions} ms on average for $n using the method 1.")

  val start2 = new ju.Date
  (1 to repetitions).foldLeft(startAccumGlobal) { case (acum, _) => calcAux2(acum)}
  println(s"Finished in ${(new ju.Date().getTime - start2.getTime) / repetitions} ms on average for $n using the method 2.")

//  val startAccumGlobalParallel = convertAccumGlobalToParallel(startAccumGlobal, groupSizes)
//  val start3 = new ju.Date
//  (1 to repetitions).foldLeft(startAccumGlobalParallel) { case (acum, _) => calcAux3(acum)}
//  println(s"Finished in ${(new ju.Date().getTime - start3.getTime) / repetitions} ms on average for $n using the method 3.")
//
//  var startAccumGlobalFuture = convertAccumGlobalToFuture(startAccumGlobal, groupSizes)
//  val start4 = new ju.Date
//  val accumGlobalFutureResult = (1 to repetitions).foldLeft(startAccumGlobalFuture) { case (acum, _) => calcAux4(acum)}
//  val pSums = Await.result(Future.sequence(accumGlobalFutureResult.pSumsWithIndeces), 5 minutes)
//  println(s"Finished in ${(new ju.Date().getTime - start4.getTime) / repetitions} ms on average for $n using the method 4.")

  val startAccumGlobalArray = convertAccumGlobalToArrayType(startAccumGlobal)
  val start5 = new ju.Date
  (1 to repetitions).foldLeft(startAccumGlobalArray) { case (acum, _) => calcAux5(acum)}
  println(s"Finished in ${(new ju.Date().getTime - start5.getTime) / repetitions} ms on average for $n using the method 5.")
}

case class PersonIterativeAccumGlobalParallel(
  sumSqSums: Seq[(Double, Double)],
  pSumsWithIndeces: ParArray[(Seq[Seq[Double]], Int)],
  count: Int
)

case class PersonIterativeAccumGlobalFuture(
  sumSqSums: Seq[(Double, Double)],
  pSumsWithIndeces: Seq[Future[(Seq[Seq[Double]], Int)]],
  count: Int
)