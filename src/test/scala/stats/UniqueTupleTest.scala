package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl._

import scala.util.Random

class UniqueTupleTest extends AsyncFlatSpec with Matchers {

  private val randomInputSize = 100000

  private val intLongCalc = UniqueTupleCalc.apply[Int, Long]
  private val doubleCalc = UniqueTupleCalc.apply[Double, Double]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Unique Tuples" should "match each other (int/long)" in {
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

  "Unique Tuples" should "match each other (double)" in {
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
    result: TupleCalcTypePack[A, B]#OUT)(
    implicit orderingA: Ordering[A], orderingB: Ordering[B]
   ) = {
    // aux function to sort pairs
    def sort(first: (A,B), second : (A,B)): Boolean =
      orderingA.lt(first._1, second._1) || ((first._1 == second._1) && (orderingB.lt(first._2, second._2)))

    result.size should be (protoResult.size)

    val size1 = result.map(_._1).size
    val size2 = protoResult.map(_._2).size

    result.map(_._1).sum should be (protoResult.map(_._1).sum)
    result.map(_._2).sum should be (protoResult.map(_._2).sum)

    result.toSeq.sortWith(sort).zip(protoResult.toSeq.sortWith(sort)).foreach { case ((a1, b1), (a2, b2)) =>
      a1 should be (a2)
      b1 should be (b2)
    }

    result.size should be (inputs.collect { case (Some(value1), Some(value2)) => (value1, value2) }.toSet.size )
  }
}