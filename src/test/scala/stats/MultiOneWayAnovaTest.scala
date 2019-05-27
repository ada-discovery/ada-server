package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{MultiOneWayAnovaTestCalc, NullExcludedMultiOneWayAnovaTestCalc, OneWayAnovaResult}

import scala.concurrent.Future
import scala.util.Random

class MultiOneWayAnovaTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val exampleInputs: Seq[(Option[String], Seq[Option[Double]])] = Seq(
    Some("x") -> Seq(Some(0.5), Some(643d)),
    Some("x") -> Seq(Some(0.7), Some(655d)),
    Some("x") -> Seq(Some(1.2), Some(702d)),
    Some("x") -> Seq(Some(6.3), None),
    Some("x") -> Seq(Some(0.1), None),

    Some("y") -> Seq(Some(0.4), None),
    Some("y") -> Seq(Some(-1.2), None),
    Some("y") -> Seq(Some(0.8), None),
    Some("y") -> Seq(Some(0.23), None),

    Some("x") -> Seq(Some(0.4), None),
    Some("x") -> Seq(Some(0.7), None),
    Some("x") -> Seq(Some(-1.2), None),

    None -> Seq(None, Some(484d)),
    None -> Seq(None, Some(456d)),

    Some("x") -> Seq(Some(3), None),
    Some("x") -> Seq(Some(4.2), None),
    Some("x") -> Seq(Some(5.7), None),

    Some("y") -> Seq(Some(0.5), Some(469d)),
    Some("y") -> Seq(Some(0.4), Some(427d)),
    Some("y") -> Seq(Some(0.4), Some(525d)),

    Some("x") -> Seq(Some(4.2), None),
    Some("x") -> Seq(Some(8.1), None),

    Some("y") -> Seq(Some(0.9), None),
    Some("y") -> Seq(Some(2), None),
    Some("y") -> Seq(Some(0.1), None),
    Some("y") -> Seq(Some(-4.1), None),
    Some("y") -> Seq(Some(3), None),
    Some("y") -> Seq(Some(4), None),

    None -> Seq(None, Some(402d))
  )

  private val expectedResult =
    Seq(
      OneWayAnovaResult(0.04405750860028412, 4.516646514922993, 1, 24),
      OneWayAnovaResult(0.0012071270284831348, 25.17541122163707, 2, 6)
    )

  private val precision = 0.00000000001

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val calc = MultiOneWayAnovaTestCalc[Option[String]]
  private val nullExcludedCalc = NullExcludedMultiOneWayAnovaTestCalc[String]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Multi one-way ANOVA test" should "match the static example" in {
    val inputSource = Source.fromIterator(() => exampleInputs.toIterator)

    // standard calculation
    Future(calc.fun_(exampleInputs)).map(checkResult(expectedResult.map(Some(_))))

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult(expectedResult.map(Some(_))))
  }

  "Multi one-way ANOVA test" should "match each other" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val values = for (i <- 1 to randomFeaturesNum) yield {
        if (Random.nextDouble() > 0.8) Some((Random.nextDouble() * 2) - 1) else None
      }
      (Some(Random.nextInt(5).toString), values)
    }

    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = calc.fun_(inputs)

    // streamed calculations
    calc.runFlow_(inputSource).map(checkResult(protoResult))

    // standard calculation (with null group values excluded)
    Future(nullExcludedCalc.fun_(inputs)).map(checkResult(protoResult))

    // streamed calculations (with null group values excluded)
    nullExcludedCalc.runFlow_(inputSource).map(checkResult(protoResult))
  }

  private def checkResult(
    results2: Seq[Option[OneWayAnovaResult]])(
    results1: Seq[Option[OneWayAnovaResult]]
  ) = {
    results1.zip(results2).map { case (result1, result2) =>
      result1 should not be (None)
      result2 should not be (None)
      val resultDefined1 = result1.get
      val resultDefined2 = result2.get

      resultDefined1.pValue shouldBeAround(resultDefined2.pValue, precision)
      resultDefined1.FValue shouldBeAround(resultDefined2.FValue, precision)
      resultDefined1.dfbg shouldBeAround(resultDefined2.dfbg, precision)
      resultDefined1.dfwg shouldBeAround(resultDefined2.dfwg, precision)
    }

    results1.size should be (results2.size)
  }
}