package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl.{BasicStatsCalc, BasicStatsResult, MultiBasicStatsCalc}
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class MultiBasicStatsTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val values: Seq[Seq[Option[Double]]] = Seq(
    Seq(None, Some(-2)),
    Seq(Some(0.5), None),
    Seq(Some(2), Some(3.1)),
    Seq(Some(-3.5), Some(9)),
    Seq(None, None),
    Seq(Some(8.9), Some(2.5)),
    Seq(Some(4.2), Some(-8.2)),
    Seq(Some(8.1), None),
    Seq(Some(0), None),
    Seq(Some(-1), Some(7.6))
  )

  private val variance1 = 22.495 - 2.4 * 2.4
  private val variance2 = 37.643333333 - 2 * 2

  private val expectedResult = Seq(
    BasicStatsResult(-3.5, 8.9, 2.4, 19.2, 179.96, variance1, Math.sqrt(variance1), variance1 * 8 / 7, Math.sqrt(variance1 * 8 / 7), 8, 2),
    BasicStatsResult(-8.2, 9,     2, 12, 225.86, variance2, Math.sqrt(variance2), variance2 * 6 / 5, Math.sqrt(variance2 * 6 / 5), 6, 4)
  )

  private val randomInputSize = 1000
  private val randomSampleNum = 1000
  private val precision = 0.00000001

  private val calc = MultiBasicStatsCalc

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Multi basic stats" should "match the static example" in {
    val inputSource = Source.fromIterator(() => values.toIterator)

    def checkResult(results: Seq[Option[BasicStatsResult]]) = {
      results.zip(expectedResult).map { case (result, expectedResult) => checkResultAux(result, Some(expectedResult)) }

      results.size should be (expectedResult.size)
    }

    // standard calculation
    Future(calc.fun_(values)).map(checkResult)

    // streamed calculation
    calc.runFlow_(inputSource).map(checkResult)
  }

  "Multi basic stats" should "match each other" in {
    val inputs =
      for (_ <- 1 to randomSampleNum) yield
        for (_ <- 1 to randomInputSize) yield
          if (Random.nextDouble < 0.2) None else Some(Random.nextDouble())

    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = calc.fun_(inputs)

    def checkResult(results: Seq[Option[BasicStatsResult]]) = {
      results.zip(protoResult).map { case (result, expectedResult) => checkResultAux(result, expectedResult)}

      results.size should be (protoResult.size)
    }

    // streamed calculation
    calc.runFlow_(inputSource).map(checkResult)
  }

  private def checkResultAux(
    result1: Option[BasicStatsResult],
    result2: Option[BasicStatsResult]
  ) = {
    result1 should not be (None)
    result2 should not be (None)

    val result1Defined = result1.get
    val result2Defined = result2.get

    result1Defined.min should be(result2Defined.min)
    result1Defined.max should be(result2Defined.max)
    result1Defined.mean shouldBeAround(result2Defined.mean, precision)
    result1Defined.sum shouldBeAround (result2Defined.sum, precision)
    result1Defined.sqSum shouldBeAround (result2Defined.sqSum, precision)
    result1Defined.variance shouldBeAround(result2Defined.variance, precision)
    result1Defined.standardDeviation shouldBeAround(result2Defined.standardDeviation, precision)
    result1Defined.sampleVariance shouldBeAround(result2Defined.sampleVariance, precision)
    result1Defined.sampleStandardDeviation shouldBeAround(result2Defined.sampleStandardDeviation, precision)
    result1Defined.definedCount should be(result2Defined.definedCount)
    result1Defined.undefinedCount should be(result2Defined.undefinedCount)
  }
}