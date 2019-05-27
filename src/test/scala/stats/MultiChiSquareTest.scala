package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{NullExcludedMultiChiSquareTestCalc, ChiSquareResult, MultiChiSquareTestCalc}

import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.Random.shuffle

class MultiChiSquareTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val featuresNum = 10

  private val inputs1: Seq[Seq[Seq[Int]]] = Seq(
    shuffleMulti(createSeq((60, 0), (54, -1), (46, 22), (41, 3))),
    shuffleMulti(createSeq((40, 0), (44, -1), (53, 22), (57, 3)))
  )

  private val expectedResult1 = (0 to featuresNum - 1).map(_ => ChiSquareResult(0.04588650089174715, 8.00606624626254, 3))

  private val inputs2: Seq[Seq[Seq[Int]]] = Seq(
    shuffleMulti(createSeq((33, 0), (29, 1))),
    shuffleMulti(createSeq((153, 0), (181, 1))),
    shuffleMulti(createSeq((103, 0), (81, 1))),
    shuffleMulti(createSeq((16, 0), (14, 1)))
  )

  private val expectedResult2 = (0 to featuresNum - 1).map(_ => ChiSquareResult(0.14667851968027895, 5.369138021292619, 3))

  def createSeq[T](counts: (Int, T)*): Seq[T] =
    shuffle(
      counts.flatMap { case (count, value) => Seq.fill(count)(value) }
    )

  def shuffleMulti[T](seq: Seq[T]) =
    (0 to featuresNum - 1).map(_ => shuffle(seq)).transpose

  private val precision = 0.00000000001

  private val randomInputSize = 5000
  private val randomFeaturesNum = 10

  private val stringIntCalc = MultiChiSquareTestCalc[Option[String], Int]
  private val definedStringIntCalc = MultiChiSquareTestCalc[String, Int]
  private val nullExcludedStringIntCalc = NullExcludedMultiChiSquareTestCalc[String, Int]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Multi Chi-Square test" should "match the static example 1" in {
    testExpectedAux(inputs1, expectedResult1)
  }

  "Multi Chi-Square test" should "match the static example 2" in {
    testExpectedAux(inputs2, expectedResult2)
  }

  private def testExpectedAux(
    rawInputs: Seq[Seq[Seq[Int]]],
    expectedResults: Seq[ChiSquareResult]
  ) = {
    val stringDefinedGroupInputs: Seq[(String, Seq[Option[Int]])] =
      rawInputs.zipWithIndex.flatMap { case (values, groupIndex) => values.map(values => (groupIndex.toString, values.map(Some(_))))}
    val groupInputs = stringDefinedGroupInputs.map { case (group, value) => (Some(group), value) }

    val inputs = shuffle(groupInputs)
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val stringDefinedInputs = shuffle(stringDefinedGroupInputs)
    val stringDefinedInputSource = Source.fromIterator(() => stringDefinedInputs.toIterator)

    val results = expectedResults.map(Some(_))

    // standard calculation
    Future(stringIntCalc.fun_(inputs)).map(checkResult(results))

    // streamed calculations
    stringIntCalc.runFlow_(inputSource).map(checkResult(results))

    // standard calculation (with string defined)
    Future(definedStringIntCalc.fun_(stringDefinedInputs)).map(checkResult(results))

    // streamed calculations (with string defined)
    definedStringIntCalc.runFlow_(stringDefinedInputSource).map(checkResult(results))

    // standard calculation (with all defined)
    Future(nullExcludedStringIntCalc.fun_(inputs)).map(checkResult(results))

    // streamed calculations (with all defined)
    nullExcludedStringIntCalc.runFlow_(inputSource).map(checkResult(results))
  }

  "Multi Chi-Square test" should "match each other" in {
    val stringDefinedInputs: Seq[(String, Seq[Option[Int]])] =
      for (_ <- 1 to randomInputSize) yield {
        val values = for (i <- 1 to randomFeaturesNum) yield {
          if (Random.nextDouble() > 0.8) Some(i + Random.nextInt(10)) else None
        }
        (Random.nextInt(5).toString, values)
      }
    val inputs = stringDefinedInputs.map { case (string, values) => (Some(string), values) }

    val stringDefinedInputSource = Source.fromIterator(() => stringDefinedInputs.toIterator)
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = stringIntCalc.fun_(inputs)

    // streamed calculations
    stringIntCalc.runFlow_(inputSource).map(checkResult(protoResult))

    // standard calculation (with string defined)
    Future(definedStringIntCalc.fun_(stringDefinedInputs)).map(checkResult(protoResult))

    // streamed calculations (with string defined)
    definedStringIntCalc.runFlow_(stringDefinedInputSource).map(checkResult(protoResult))
  }

  private def checkResult(
    results2: Seq[Option[ChiSquareResult]])(
    results1: Seq[Option[ChiSquareResult]]
  ) = {
    results1.zip(results2).map { case (result1, result2) =>
      result1 should not be (None)
      result2 should not be (None)
      val resultDefined1 = result1.get
      val resultDefined2 = result2.get

      resultDefined1.pValue shouldBeAround(resultDefined2.pValue, precision)
      resultDefined1.statistics shouldBeAround(resultDefined2.statistics, precision)
      resultDefined1.degreeOfFreedom shouldBeAround(resultDefined2.degreeOfFreedom, precision)
    }

    results1.size should be (results2.size)
  }
}