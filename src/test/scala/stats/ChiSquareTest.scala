package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{ChiSquareResult, ChiSquareTestCalc}

import scala.concurrent.Future
import scala.util.Random
import Seq.fill
import scala.util.Random.shuffle

class ChiSquareTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val inputs1: Seq[Seq[Double]] = Seq(
    createSeq((60, 0), (54, -1), (46, 22), (41, 3.5)),
    createSeq((40, 0), (44, -1), (53, 22), (57, 3.5))
  )
  private val expectedResult1 = ChiSquareResult(0.04588650089174715, 8.00606624626254, 3)

  private val inputs2: Seq[Seq[Double]] = Seq(
    createSeq((33, 0), (29, 1)),
    createSeq((153, 0), (181, 1)),
    createSeq((103, 0), (81, 1)),
    createSeq((16, 0), (14, 1))
  )

  private val expectedResult2 = ChiSquareResult(0.14667851968027895, 5.369138021292619, 3)

  def createSeq[T](counts: (Int, T)*): Seq[T] =
    shuffle(
      counts.flatMap { case (count, value) => fill(count)(value) }
    )

  private val precision = 0.00000000001

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val stringDoubleCalc = ChiSquareTestCalc[Option[String], Option[Double]]
  private val stringIntCalc = ChiSquareTestCalc[Option[String], Option[Int]]

  private val allDefinedStringDoubleCalc = ChiSquareTestCalc[String, Double]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Chi-Square test" should "match the static example 1" in {
    testExpectedAux(inputs1, expectedResult1)
  }

  "Chi-Square test" should "match the static example 2" in {
    testExpectedAux(inputs2, expectedResult2)
  }

  private def testExpectedAux(
    rawInputs: Seq[Seq[Double]],
    expectedResult: ChiSquareResult
  ) = {
    val allDefinedGroupInputs: Seq[(String, Double)] = rawInputs.zipWithIndex.flatMap { case (values, groupIndex) => values.map(value => (groupIndex.toString, value))}
    val groupInputs = allDefinedGroupInputs.map { case (group, value) => (Some(group), Some(value))}

    val inputs = shuffle(groupInputs)
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val allDefinedInputs = shuffle(allDefinedGroupInputs)
    val allDefinedInputSource = Source.fromIterator(() => allDefinedInputs.toIterator)

    // standard calculation
    Future(stringDoubleCalc.fun_(inputs)).map(checkResult(Some(expectedResult)))

    // streamed calculations
    stringDoubleCalc.runFlow_(inputSource).map(checkResult(Some(expectedResult)))

    // all-defined standard calculation
    Future(allDefinedStringDoubleCalc.fun_(allDefinedInputs)).map(checkResult(Some(expectedResult)))

    // all-defined streamed calculations
    allDefinedStringDoubleCalc.runFlow_(allDefinedInputSource).map(checkResult(Some(expectedResult)))
  }

  "Chi-Square test" should "match each other" in {
    val allInputs = for (_ <- 1 to randomInputSize) yield {
      for (i <- 1 to randomFeaturesNum) yield {
        val value = if (Random.nextDouble() > 0.8) Some(Random.nextInt(10)) else None
        (Some(i.toString), value)
      }
    }
    val inputs = allInputs.flatten
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = stringIntCalc.fun_(inputs)

    // streamed calculations
    stringIntCalc.runFlow_(inputSource).map(checkResult(protoResult))
  }

  private def checkResult(
    result2: Option[ChiSquareResult])(
    result1: Option[ChiSquareResult]
  ) = {
    result1 should not be (None)
    result2 should not be (None)
    val resultDefined1 = result1.get
    val resultDefined2 = result2.get

    resultDefined1.pValue shouldBeAround (resultDefined2.pValue, precision)
    resultDefined1.statistics shouldBeAround (resultDefined2.statistics, precision)
    resultDefined1.degreeOfFreedom shouldBeAround (resultDefined2.degreeOfFreedom, precision)
  }
}