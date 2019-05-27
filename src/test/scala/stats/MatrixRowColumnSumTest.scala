package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl.MatrixRowColumnSumCalc
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class MatrixRowColumnSumTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val inputs =
    Seq(
      Seq(0.5,  0.7,  1.2,  6.3,  0.1),
      Seq( -2,    0,  7.1, -3.1,  4.2),
      Seq(1.2,  0.2,  1.2,  6.3, -9.9),
      Seq(0.8,  0.7, -8.3,  2.2,  0.1),
      Seq(4.9, -1.1,  1.2,  0.1,  3.2)
    )

  private val expectedResult = (
    Seq(8.8, 6.2,  -1, -4.5,  8.3),
    Seq(5.4, 0.5, 2.4, 11.8, -2.3)
  )

  private val randomInputSize = 100
  private val precision = 0.00000001

  private val calc = MatrixRowColumnSumCalc.apply

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Matrix row column sums" should "match the static example" in {
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkResult(rowColumnSums: (Seq[Double], Seq[Double])) = {
      val rowSums = rowColumnSums._1
      val columnSums = rowColumnSums._2

      (rowSums, expectedResult._1).zipped.foreach { case (sum1, sum2) =>
        sum1 shouldBeAround (sum2, precision)
      }

      (columnSums, expectedResult._2).zipped.foreach { case (sum1, sum2) =>
        sum1 shouldBeAround (sum2, precision)
      }

      rowSums.size should be (inputs.size)
      columnSums.size should be (inputs.size)

      rowSums.sum shouldBeAround (columnSums.sum, precision)
    }

    // standard calculation
    Future(calc.fun_(inputs)).map(checkResult)

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult)
  }

  "Matrix row column sums" should "match each other" in {
    val inputs =
      for (_ <- 1 to randomInputSize) yield {
        for (_ <- 1 to randomInputSize) yield
          (Random.nextDouble() * 2) - 1
      }

    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val (protoRowSums, protoColumnSums) = calc.fun_(inputs)

    def checkResult(rowColumnSums: (Seq[Double], Seq[Double])) = {
      val rowSums = rowColumnSums._1
      val columnSums = rowColumnSums._2

      (rowSums, protoRowSums).zipped.foreach { case (sum1, sum2) =>
        sum1 should be (sum2)
      }

      (columnSums, protoColumnSums).zipped.foreach { case (sum1, sum2) =>
        sum1 should be (sum2)
      }

      rowSums.size should be (protoRowSums.size)
      columnSums.size should be (protoColumnSums.size)

      rowSums.size should be (randomInputSize)
      columnSums.size should be (randomInputSize)

      rowSums.sum shouldBeAround (columnSums.sum, precision)
    }

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult)
  }
}