package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl.{AllDefinedEuclideanDistanceCalc, EuclideanDistanceCalc}
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class EuclideanDistanceTest extends AsyncFlatSpec with Matchers {

  private val xs = Seq(0.5, 0.7, 1.2, 6.3,  0.1, 0.4,  0.7, -1.2, 3, 4.2,  5.7, 4.2, 8.1)
  private val ys = Seq(0.5, 0.4, 0.4, 0.4, -1.2, 0.8, 0.23,  0.9, 2, 0.1, -4.1,   3, 4)
  private val expectedResult = Math.sqrt(174.1209)

  private val randomInputSize = 100
  private val randomFeaturesNum = 25

  private val calc = EuclideanDistanceCalc
  private val allDefinedCalc = AllDefinedEuclideanDistanceCalc

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Euclidean distances" should "match the static example" in {
    val inputs = xs.zip(ys).map{ case (a,b) => Seq(Some(a),Some(b))}
    val inputsAllDefined = xs.zip(ys).map{ case (a,b) => Seq(a, b)}
    val inputSource = Source.fromIterator(() => inputs.toIterator)
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)

    def checkResult(result: Seq[Seq[Double]]) = {
      result.size should be (2)
      result.map(_.size should be (2))
      result(0)(0) should be (0d)
      result(1)(1) should be (0d)
      result(0)(1) should be (expectedResult)
      result(1)(0) should be (expectedResult)
    }

    val featuresNum = inputs.head.size

    // standard calculation
    Future(calc.fun()(inputs)).map(checkResult)

    // streamed calculations without parallelism
    calc.runFlow(None, ())(inputSource).map(checkResult)

    // streamed calculations with parallelism
    calc.runFlow(Some(4), ())(inputSource).map(checkResult)

    // all-values-defined streamed calculations without parallelism
    allDefinedCalc.runFlow(None, ())(inputSourceAllDefined).map(checkResult)

    // all-values-defined streamed calculations with parallelism
    allDefinedCalc.runFlow(Some(4), ())(inputSourceAllDefined).map(checkResult)
  }

  "Euclidean distances" should "match each other" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      for (_ <- 1 to randomFeaturesNum) yield
       Some((Random.nextDouble() * 2) - 1)
    }
    val inputsAllDefined = inputs.map(_.map(_.get))
    val inputSource = Source.fromIterator(() => inputs.toIterator)
    val inputSourceAllDefined = Source.fromIterator(() => inputsAllDefined.toIterator)

    // standard calculation
    val protoResult = calc.fun()(inputs)

    def checkResult(result: Seq[Seq[Double]]) = {
      result.map(_.size should be (randomFeaturesNum))
      for (i <- 0 to randomFeaturesNum - 1) yield
        result(i)(i) should be (0d)

      result.zip(protoResult).map { case (rowCor1, rowCor2) =>
        rowCor1.zip(rowCor2).map { case (cor1, cor2) =>
          cor1 should be (cor2)
        }
      }
      result.size should be (randomFeaturesNum)
    }

    // streamed calculations without parallelism
    calc.runFlow(None, ())(inputSource).map(checkResult)

    // streamed calculations with parallelism
    calc.runFlow(Some(4), ())(inputSource).map(checkResult)

    // all-values-defined streamed calculations without parallelism
    allDefinedCalc.runFlow(None, ())(inputSourceAllDefined).map(checkResult)

    // all-values-defined streamed calculations with parallelism
    allDefinedCalc.runFlow(Some(4), ())(inputSourceAllDefined).map(checkResult)
  }
}