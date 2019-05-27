package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl.{BasicStatsCalc, BasicStatsResult}
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class BasicStatsTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val values: Seq[Option[Double]] = Seq(None, Some(0.5), Some(2), Some(-3.5), None, Some(8.9), Some(4.2), Some(8.1), Some(0), Some(-1))

  private val variance = 22.495 - 2.4 * 2.4
  private val expectedResult = BasicStatsResult(
    -3.5, 8.9, 2.4, 19.2, 179.96, variance, Math.sqrt(variance), variance * 8 / 7, Math.sqrt(variance * 8 / 7), 8, 2
  )

  private val randomInputSize = 1000
  private val precision = 0.00000001

  private val calc = BasicStatsCalc

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Basic stats" should "match the static example" in {
    val inputSource = Source.fromIterator(() => values.toIterator)

    // standard calculation
    Future(calc.fun_(values)).map(checkResult(Some(expectedResult)))

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult(Some(expectedResult)))
  }

  "Basic stats" should "match each other" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      if (Random.nextDouble < 0.2) None else Some(Random.nextDouble())
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = calc.fun_(inputs).get

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult(Some(protoResult)))
  }

  private def checkResult(
    result2: Option[BasicStatsResult])(
    result1: Option[BasicStatsResult]
  ) = {
    result1 should not be (None)
    result2 should not be (None)
    val resultDefined1 = result1.get
    val resultDefined2 = result2.get

    resultDefined1.min should be (resultDefined2.min)
    resultDefined1.max should be (resultDefined2.max)
    resultDefined1.mean shouldBeAround (resultDefined2.mean, precision)
    resultDefined1.sum shouldBeAround (resultDefined2.sum, precision)
    resultDefined1.sqSum shouldBeAround (resultDefined2.sqSum, precision)
    resultDefined1.variance shouldBeAround (resultDefined2.variance, precision)
    resultDefined1.standardDeviation shouldBeAround (resultDefined2.standardDeviation, precision)
    resultDefined1.sampleVariance shouldBeAround (resultDefined2.sampleVariance, precision)
    resultDefined1.sampleStandardDeviation shouldBeAround (resultDefined2.sampleStandardDeviation, precision)
    resultDefined1.definedCount should be (resultDefined2.definedCount)
    resultDefined1.undefinedCount should be (resultDefined2.undefinedCount)
  }
}