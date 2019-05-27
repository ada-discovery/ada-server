package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.CalculatorHelper._
import org.ada.server.calc.impl._

import scala.util.Random

class GroupUniqueTupleTest extends AsyncFlatSpec with Matchers {

  private val randomInputSize = 100000

  private val intLongCalc = GroupUniqueTupleCalc.apply[String, Int, Long]
  private val doubleCalc = GroupUniqueTupleCalc.apply[String, Double, Double]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Tuples" should "match each other (int/long)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value1 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20))
      val value2 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toLong)
      (group, value1, value2)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = intLongCalc.fun_(inputs)

    // streamed calculations
    intLongCalc.runFlow_(inputSource).map(checkResult(protoResult, inputs))
  }

  "Tuples" should "match each other (double)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value1 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      val value2 = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      (group, value1, value2)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = doubleCalc.fun_(inputs)

    // streamed calculations
    doubleCalc.runFlow_(inputSource).map(checkResult(protoResult, inputs))
  }

  def checkResult[G: Ordering, A: Numeric, B: Numeric](
    protoResult: GroupTupleCalcTypePack[G, A, B]#OUT,
    inputs: Traversable[(Option[G], Option[A], Option[B])])(
    result: GroupTupleCalcTypePack[G, A, B]#OUT)(
    implicit orderingA: Ordering[A], orderingB: Ordering[B]
   ) = {
    // aux function to sort pairs
    def sort(first: (A,B), second : (A,B)): Boolean =
      orderingA.lt(first._1, second._1) || ((first._1 == second._1) && (orderingB.lt(first._2, second._2)))

    result.size should be (protoResult.size)

    result.toSeq.sortBy(_._1).zip(protoResult.toSeq.sortBy(_._1)).foreach{ case ((groupName1, values1), (groupName2, values2)) =>
      groupName1 should be (groupName2)
      values1.size should be (values2.size)

      values1.map(_._1).sum should be (values2.map(_._1).sum)
      values1.map(_._2).sum should be (values2.map(_._2).sum)

      values1.toSeq.sortWith(sort).zip(values2.toSeq.sortWith(sort)).foreach { case ((a1, b1), (a2, b2)) =>
        a1 should be (a2)
        b1 should be (b2)
      }
    }

    result.map(_._2.size).sum should be (inputs.collect { case (g, Some(value1), Some(value2)) => (g, value1, value2) }.toSet.size)
  }
}