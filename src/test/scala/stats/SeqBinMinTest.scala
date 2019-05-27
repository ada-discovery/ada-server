package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{AllDefinedSeqBinMinCalc, NumericDistributionFlowOptions, NumericDistributionOptions, SeqBinMinCalc}

import scala.concurrent.Future
import scala.util.Random

class SeqBinMinTest extends AsyncFlatSpec with Matchers {

  private val xs = Seq(3.2, 8.9, -1.2, 3.7, -10.8, 12.3, -0.1, 0, 5.1)

  private val values = Seq(
    Seq(  0.5,  0.7,  1.2, xs(0)),
    Seq(  2.5,  0.4,  1.5, xs(1)),
    Seq( -2.6,  6.9,  6.4, xs(2)),
    Seq(  2.8, -2.1,  0.4, xs(3)),
    Seq(  0.5,    0,  0.4, xs(4)),
    Seq(  -12,  0.9,  5.6, xs(5)),
    Seq(    3,  0.1,  3.2, xs(6)),
    Seq(-11.5, -1.1,  5.1, xs(7)),
    Seq(-10.1,  1.3,  4.8, xs(8))
  )

  private val expectedResult = Seq(
    Seq(-12, -2.1, 0.4) -> None,
    Seq(-12, -2.1, 2.4) -> None,
    Seq(-12, -2.1, 4.4) -> Some(xs(7)),

    Seq(-12, 0.9, 0.4) -> None,
    Seq(-12, 0.9, 2.4) -> None,
    Seq(-12, 0.9, 4.4) -> Some(Seq(xs(5), xs(8)).min),

    Seq(-12, 3.9, 0.4) -> None,
    Seq(-12, 3.9, 2.4) -> None,
    Seq(-12, 3.9, 4.4) -> None,

    Seq(-7, -2.1, 0.4) -> None,
    Seq(-7, -2.1, 2.4) -> None,
    Seq(-7, -2.1, 4.4) -> None,

    Seq(-7, 0.9, 0.4) -> None,
    Seq(-7, 0.9, 2.4) -> None,
    Seq(-7, 0.9, 4.4) -> None,

    Seq(-7, 3.9, 0.4) -> None,
    Seq(-7, 3.9, 2.4) -> None,
    Seq(-7, 3.9, 4.4) -> Some(xs(2)),

    Seq(-2, -2.1, 0.4) -> Some(Seq(xs(0), xs(1), xs(3), xs(4)).min),
    Seq(-2, -2.1, 2.4) -> Some(xs(6)),
    Seq(-2, -2.1, 4.4) -> None,

    Seq(-2, 0.9, 0.4) -> None,
    Seq(-2, 0.9, 2.4) -> None,
    Seq(-2, 0.9, 4.4) -> None,

    Seq(-2, 3.9, 0.4) -> None,
    Seq(-2, 3.9, 2.4) -> None,
    Seq(-2, 3.9, 4.4) -> None
  )

//  Seq(-12, -7, -2),
//  Seq(-2.1, 1.1, 4.1),
//  Seq(0.4, 2.4, 4.4)

  private val binCount1 = 3

  private val randomInputSize = 10000
  private val randomFeaturesNum = 8

  private val calc = SeqBinMinCalc.apply
  private val allDefinedCalc = AllDefinedSeqBinMinCalc.apply

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Seq bin mins" should "match the static example" in {
    val inputs = values.map(_.map(Some(_)))
    val inputsAllDefined = values
    val inputSource = Source.fromIterator(() => inputs.toIterator)
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)

    def checkResult(result: Traversable[(Seq[BigDecimal], Option[Double])]) = {
      result.size should be (expectedResult.size)

      expectedResult.zip(result.toSeq).map { case (expectedResult, result) =>
        result._1.map(_.toDouble).zip(expectedResult._1).map { case (a, b) =>
          a should be (b)
        }
        result._2 should be (expectedResult._2)
      }

      result.flatMap(_._2).sum  should be (expectedResult.flatMap(_._2).sum)
    }


    val standardOptions = NumericDistributionOptions(binCount1)

    // standard calculation
    Future(calc.fun(Seq(standardOptions, standardOptions, standardOptions))(inputs)).map(checkResult)

    // standard calculation all defined
    Future(allDefinedCalc.fun(Seq(standardOptions, standardOptions, standardOptions))(inputsAllDefined)).map(checkResult)

    val streamOptions =
      for (index <- 0 to 2) yield {
        val oneDimValues = inputsAllDefined.map(_(index))
        NumericDistributionFlowOptions(binCount1, oneDimValues.min, oneDimValues.max)
      }

    // streamed calculations
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)

    // streamed calculations all defined
    allDefinedCalc.runFlow(streamOptions, streamOptions)(inputSourceAllDefined).map(checkResult)
  }

  "Seq bin mins" should "match each other" in {
    val inputsAllDefined = for (_ <- 1 to randomInputSize) yield {
      for (_ <- 1 to randomFeaturesNum) yield (Random.nextDouble() * 2) - 1
    }
    val inputs = inputsAllDefined.map(_.map(Some(_)))
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val binCounts = for (_ <- 1 to randomFeaturesNum) yield Random.nextInt(4) + 2
    val totalSize = binCounts.foldLeft(1d){_*_}

    // standard calculation
    val standardOptions = binCounts.map(NumericDistributionOptions(_))
    val protoResult = calc.fun(standardOptions)(inputs)

    def checkResult(result: Traversable[(Seq[BigDecimal], Option[Double])]) = {
      result.map(_._1.size should be (randomFeaturesNum))

      result.toSeq.zip(protoResult.toSeq).map { case (row1, row2) =>
        row1._2 should be (row2._2)

        row1._1.zip(row2._1).map { case (index1, index2) =>
          index1 should be (index2)
        }
      }
      result.size should be (totalSize)
    }

    // standard calculation all defined
    Future(allDefinedCalc.fun(standardOptions)(inputsAllDefined)).map(checkResult)

    val streamOptions = binCounts.zipWithIndex.map { case (binCount, index) =>
      val oneDimValues = inputsAllDefined.map(_ (index))
      NumericDistributionFlowOptions(binCount, oneDimValues.min, oneDimValues.max)
    }

    // streamed calculations
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)

    // streamed calculations all defined
    allDefinedCalc.runFlow(streamOptions, streamOptions)(inputSourceAllDefined).map(checkResult)
  }
}