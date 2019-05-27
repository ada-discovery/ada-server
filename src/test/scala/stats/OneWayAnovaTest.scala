package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{OneWayAnovaTestCalc, OneWayAnovaResult}

import scala.concurrent.Future
import scala.util.Random
import scala.util.Random.shuffle

class OneWayAnovaTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val inputs1 = Seq(
    Seq(0.5, 0.7, 1.2, 6.3, 0.1, 0.4, 0.7, -1.2, 3, 4.2, 5.7, 4.2, 8.1),
    Seq(0.5, 0.4, 0.4, 0.4, -1.2, 0.8, 0.23, 0.9, 2, 0.1, -4.1, 3, 4)
  )
  private val expectedResult1 = OneWayAnovaResult(0.04405750860028412, 4.516646514922993, 1, 24)

  private val inputs2 = Seq(
    Seq(643d, 655d, 702d),
    Seq(469d, 427d, 525d),
    Seq(484d, 456d, 402d)
  )
  private val expectedResult2 = OneWayAnovaResult(0.0012071270284831348, 25.17541122163707, 2, 6)

  private val precision = 0.00000000001

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val stringCalc = OneWayAnovaTestCalc[Option[String]]
  private val definedStringCalc = OneWayAnovaTestCalc[String]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "One-way ANOVA test" should "match the static example 1" in {
    testExpectedAux(inputs1, expectedResult1)
  }

  "One-way ANOVA test" should "match the static example 2" in {
    testExpectedAux(inputs2, expectedResult2)
  }

  private def testExpectedAux(
    rawInputs: Seq[Seq[Double]],
    expectedResult: OneWayAnovaResult
  ) = {
    val definedGroupInputs = rawInputs.zipWithIndex.flatMap { case (values, groupIndex) => values.map(value => (groupIndex.toString, Some(value)))}
    val groupInputs = definedGroupInputs.map { case (group, value) => (Some(group), value) }

    val inputs = shuffle(groupInputs)
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val definedInputs = shuffle(definedGroupInputs)
    val definedInputSource = Source.fromIterator(() => definedInputs.toIterator)

    // standard calculation
    Future(stringCalc.fun_(inputs)).map(checkResult(Some(expectedResult)))

    // streamed calculations
    stringCalc.runFlow_(inputSource).map(checkResult(Some(expectedResult)))

    // standard calculation (with defined group)
    Future(definedStringCalc.fun_(definedInputs)).map(checkResult(Some(expectedResult)))

    // streamed calculations (with defined group)
    definedStringCalc.runFlow_(definedInputSource).map(checkResult(Some(expectedResult)))
  }

  "One-way ANOVA test" should "match each other" in {
    val allDefinedInputs = for (_ <- 1 to randomInputSize) yield {
      for (i <- 1 to randomFeaturesNum) yield {
        val value = if (Random.nextDouble() > 0.8) Some((Random.nextDouble() * 2) - 1) else None
        (i.toString, value)
      }
    }

    val definedInputs = allDefinedInputs.flatten
    val definedInputSource = Source.fromIterator(() => definedInputs.toIterator)

    val inputs = definedInputs.map { case (group, value) => (Some(group), value) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = stringCalc.fun_(inputs)

    // streamed calculations
    stringCalc.runFlow_(inputSource).map(checkResult(protoResult))

    // standard calculation (with defined group)
    Future(definedStringCalc.fun_(definedInputs)).map(checkResult(protoResult))

    // streamed calculations (with defined group)
    definedStringCalc.runFlow_(definedInputSource).map(checkResult(protoResult))
  }

  private def checkResult(
    result2: Option[OneWayAnovaResult])(
    result1: Option[OneWayAnovaResult]
  ) = {
    result1 should not be (None)
    result2 should not be (None)
    val resultDefined1 = result1.get
    val resultDefined2 = result2.get

    resultDefined1.pValue shouldBeAround (resultDefined2.pValue, precision)
    resultDefined1.FValue shouldBeAround (resultDefined2.FValue, precision)
    resultDefined1.dfbg shouldBeAround (resultDefined2.dfbg, precision)
    resultDefined1.dfwg shouldBeAround (resultDefined2.dfwg, precision)
  }
}