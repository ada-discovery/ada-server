package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl._

import scala.util.Random

class TupleTest extends AsyncFlatSpec with Matchers {

  private val randomInputSize = 100000

  private val intLongCalc = TupleCalc.apply[Int, Long]
  private val doubleCalc = TupleCalc.apply[Double, Double]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Tuples" should "match each other (int/long)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val value1 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20))
      val value2 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toLong)
      (value1, value2)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = intLongCalc.fun_(inputs)

    // streamed calculations
    intLongCalc.runFlow_(inputSource).map(checkResult(protoResult, inputs))
  }

  "Tuples" should "match each other (double)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val value1 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      val value2 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      (value1, value2)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = doubleCalc.fun_(inputs)

    // streamed calculations
    doubleCalc.runFlow_(inputSource).map(checkResult(protoResult, inputs))
  }

  def checkResult[A: Numeric, B: Numeric](
    protoResult: TupleCalcTypePack[A, B]#OUT,
    inputs: Traversable[(Option[A], Option[B])])(
    result: TupleCalcTypePack[A, B]#OUT
   ) = {
    result.size should be (protoResult.size)

    result.map(_._1).sum should be (protoResult.map(_._1).sum)
    result.map(_._2).sum should be (protoResult.map(_._2).sum)

    result.toSeq.zip(protoResult.toSeq).foreach { case ((a1, b1), (a2, b2)) =>
      a1 should be (a2)
      b1 should be (b2)
    }

    result.size should be (inputs.count { case (value1, value2) => value1.isDefined && value2.isDefined} )
  }
}