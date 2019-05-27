package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.MatthewsBinaryClassCorrelationCalc

import scala.concurrent.Future
import scala.util.Random

class MatthewsBinaryClassCorrelationTest extends AsyncFlatSpec with Matchers {

  private val exampleInputs = Seq(
    (Some(true), Some(true)),
    (Some(false), Some(false)),
    (Some(true), Some(false)),
    (None, None),
    (Some(false), Some(false)),
    (Some(false), Some(true)),
    (Some(false), Some(false)),
    (Some(false), None),
    (Some(true), Some(true)),
    (None, Some(false)),
    (Some(true), Some(true)),
    (Some(false), Some(false)),
    (Some(true), Some(false))
  )

  private val expectedResult = 0.408248290463863

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val calc = MatthewsBinaryClassCorrelationCalc

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Matthews correlations" should "match the static example" in {
    val inputs = exampleInputs.map { case (a, b) => Seq(a, b) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkResult(result: Seq[Seq[Option[Double]]]) = {
      result.size should be (2)
      result.map(_.size should be (2))
      result(0)(0).get should be (1d)
      result(1)(1).get should be (1d)
      result(0)(1).get should be (expectedResult)
      result(1)(0).get should be (expectedResult)
    }

    // standard calculation
    Future(calc.fun()(inputs)).map(checkResult)

    // streamed calculations
    calc.runFlow(None, None)(inputSource).map(checkResult)

    // parallel streamed calculations
    calc.runFlow(Some(4), Some(4))(inputSource).map(checkResult)
  }

  "Matthews correlations" should "match each other" in {
    val inputs =
      for (_ <- 1 to randomInputSize) yield {
        for (_ <- 1 to randomFeaturesNum) yield
          if (Random.nextDouble() < 0.8) Some(Random.nextBoolean) else None
      }

    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = calc.fun()(inputs)

    def checkResult(result: Seq[Seq[Option[Double]]]) = {
      result.map(_.size should be (randomFeaturesNum))

      for (i <- 0 to randomFeaturesNum - 1) yield
        result(i)(i).get should be (1d)

      result.zip(protoResult).map { case (rowCor1, rowCor2) =>
        rowCor1.zip(rowCor2).map { case (cor1, cor2) =>
          cor1 should be (cor2)
        }
      }
      result.size should be (randomFeaturesNum)
    }

    // streamed calculations
    calc.runFlow(None, None)(inputSource).map(checkResult)

    // parallel streamed calculations
    calc.runFlow(Some(4), Some(4))(inputSource).map(checkResult)
  }
}