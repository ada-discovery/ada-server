import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.services.StatsService
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl.{AllDefinedPearsonCorrelationCalc, PearsonCorrelationCalc}

import scala.concurrent.Future
import scala.util.Random

class PearsonCorrelationTest extends AsyncFlatSpec with Matchers {

  private val xs = Seq(0.5, 0.7, 1.2, 6.3, 0.1, 0.4, 0.7, -1.2, 3, 4.2, 5.7, 4.2, 8.1)
  private val ys = Seq(0.5, 0.4, 0.4, 0.4, -1.2, 0.8, 0.23, 0.9, 2, 0.1, -4.1, 3, 4)
  private val expectedResult = 0.1725323796730674

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val calc = PearsonCorrelationCalc
  private val allDefinedCalc = AllDefinedPearsonCorrelationCalc

//  private val injector = TestApp.apply.injector
//  private val statsService = injector.instanceOf[StatsService]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Correlations" should "match the static example" in {
    val inputs = xs.zip(ys).map{ case (a,b) => Seq(Some(a),Some(b))}
    val inputsAllDefined = xs.zip(ys).map{ case (a,b) => Seq(a, b)}
    val inputSource = Source.fromIterator(() => inputs.toIterator)
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)

    def checkResult(result: Seq[Seq[Option[Double]]]) = {
      result.size should be (2)
      result.map(_.size should be (2))
      result(0)(0).get should be (1d)
      result(1)(1).get should be (1d)
      result(0)(1).get should be (expectedResult)
      result(1)(0).get should be (expectedResult)
    }

    val featuresNum = inputs.head.size

    // standard calculation
    Future(calc.fun()(inputs)).map(checkResult)

    // streamed calculations
    calc.runFlow(None, None)(inputSource).map(checkResult)

    // streamed calculations with an all-values-defined optimization
    allDefinedCalc.runFlow(None, None)(inputSourceAllDefined).map(checkResult)

    // parallel streamed calculations
    calc.runFlow(Some(4), Some(4))(inputSource).map(checkResult)

    // parallel streamed calculations with an all-values-defined optimization
    allDefinedCalc.runFlow(Some(4), Some(4))(inputSourceAllDefined).map(checkResult)
  }

  "Correlations" should "match each other" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      for (_ <- 1 to randomFeaturesNum) yield
       Some((Random.nextDouble() * 2) - 1)
    }
    val inputsAllDefined = inputs.map(_.map(_.get))
    val inputSource = Source.fromIterator(() => inputs.toIterator)
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)

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

    // streamed calculations with an all-values-defined optimization
    allDefinedCalc.runFlow(None, None)(inputSourceAllDefined).map(checkResult)

    // parallel streamed calculations
    calc.runFlow(Some(4), Some(4))(inputSource).map(checkResult)

    // parallel streamed calculations with an all-values-defined optimization
    allDefinedCalc.runFlow(Some(4), Some(4))(inputSourceAllDefined).map(checkResult)
  }
}