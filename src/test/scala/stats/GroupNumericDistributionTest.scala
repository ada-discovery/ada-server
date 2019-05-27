package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl._
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class GroupNumericDistributionTest extends AsyncFlatSpec with Matchers {

  private val values1: Seq[(Option[String], Double)] = Seq(
    (Some("x"), 0.5),
    (Some("y"), 0.5),
    (Some("y"), 5),
    (Some("x"), 0.5),
    (Some("x"), 1.5),
    (Some("x"), 2),
    (None, 7.5),
    (Some("x"), 0.6),
    (Some("x"), 2.4),
    (Some("y"), 2.4),
    (Some("x"), 2.6),
    (None, 1),
    (Some("x"), 3),
    (Some("x"), 5),
    (Some("y"), 5),
    (Some("x"), 7.5),
    (Some("y"), 1.5),
    (Some("x"), 1.1),
    (None, 0.5),
    (Some("x"), 2),
    (Some("y"), 7.5)
  )

  private val expectedResult1 = Seq(
    None -> Seq(0.5 -> 2, 1.5 -> 0, 2.5 -> 0, 3.5 -> 0, 4.5 -> 0, 5.5 -> 0, 6.5 -> 1),
    Some("x") -> Seq(0.5 -> 4, 1.5 -> 4, 2.5 -> 2, 3.5 -> 0, 4.5 -> 1, 5.5 -> 0, 6.5 -> 1),
    Some("y") -> Seq(0.5 -> 1, 1.5 -> 2, 2.5 -> 0, 3.5 -> 0, 4.5 -> 2, 5.5 -> 0, 6.5 -> 1)
  )

  private val columnCount1 = 7

  private val values2: Seq[(Option[String], Option[Long])] = Seq(
    (Some("x"), Some(1)),
    (Some("x"), None),
    (None, Some(7)),
    (Some("x"), Some(1)),
    (Some("x"), Some(3)),
    (None, None),
    (Some("x"), Some(2)),
    (None, Some(1)),
    (Some("x"), None),
    (Some("x"), Some(3)),
    (Some("x"), Some(2)),
    (None, None),
    (Some("y"), Some(1)),
    (None, Some(7)),
    (Some("x"), Some(2)),
    (Some("x"), Some(3)),
    (None, Some(2)),
    (Some("x"), Some(5)),
    (Some("y"), Some(4)),
    (Some("x"), Some(7)),
    (Some("y"), None),
    (None, None),
    (Some("x"), Some(4)),
    (Some("x"), Some(2)),
    (Some("y"), Some(1)),
    (Some("x"), None),
    (Some("x"), None),
    (Some("y"), Some(7))
  )

  private val expectedResult2 = Seq(
    None -> Seq(1 -> 1, 2 -> 1, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0, 7 -> 2),
    Some("x") -> Seq(1 -> 2, 2 -> 4, 3 -> 3, 4 -> 1, 5 -> 1, 6 -> 0, 7 -> 1),
    Some("y") -> Seq(1 -> 2, 2 -> 0, 3 -> 0, 4 -> 1, 5 -> 0, 6 -> 0, 7 -> 1)
  )

  private val columnCount2 = 7

  private val randomInputSize = 1000

  private val calc = GroupNumericDistributionCountsCalc[String]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Distributions" should "match the static example (double)" in {
    val inputs = values1.map{ case (group, value) => (group, Some(value)) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkResult(result: Traversable[(Option[String], Traversable[(BigDecimal, Int)])]) = {
      result.size should be (expectedResult1.size)

      val sorted = result.toSeq.sortBy(_._1)

      sorted.zip(expectedResult1).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)
        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        counts1.toSeq.zip(counts2).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (values1.size)
    }

    // standard calculation
    val standardOptions = NumericDistributionOptions(columnCount1)
    Future(calc.fun(standardOptions)(inputs)).map(checkResult)

    // streamed calculations
    val streamOptions = NumericDistributionFlowOptions(columnCount1, values1.map(_._2).min, values1.map(_._2).max)
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)
  }

  "Distributions" should "match the static example (int/long)" in {
    val inputs = values2.map{ case (group, value) => (group, value.map(_.toDouble)) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkResult(result: Traversable[(Option[String], Traversable[(BigDecimal, Int)])]) = {
      result.size should be (expectedResult2.size)

      val sorted = result.toSeq.sortBy(_._1)

      sorted.zip(expectedResult2).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)
        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        counts1.toSeq.zip(counts2).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (values2.count(_._2.isDefined))
    }

    // standard calculation
    val standardOptions = NumericDistributionOptions(columnCount2, true)
    Future(calc.fun(standardOptions)(inputs)).map(checkResult)

    // streamed calculations
    val streamOptions = NumericDistributionFlowOptions(columnCount2, inputs.flatMap(_._2).min, inputs.flatMap(_._2).max, true)
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)
  }

  "Distributions" should "match each other (double)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      (group,  value)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val columnCount = 30

    // standard calculation
    val standardOptions = NumericDistributionOptions(columnCount)
    val protoResult = calc.fun(standardOptions)(inputs)

    def checkResult(result: Traversable[(Option[String], Traversable[(BigDecimal, Int)])]) = {
      result.size should be (protoResult.size)

      result.toSeq.zip(protoResult.toSeq).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)
        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        counts1.toSeq.zip(counts2.toSeq).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (inputs.count(_._2.isDefined))
    }

    // streamed calculations
    val streamOptions = NumericDistributionFlowOptions(columnCount, inputs.flatMap(_._2).min, inputs.flatMap(_._2).max)
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)
  }

  "Distributions" should "match each other (int/long)" in {
    val intInputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toLong)
      (group,  value)
    }

    val inputs = intInputs.map{ case (group, value) => (group, value.map(_.toDouble)) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    val columnCount = 15

    // standard calculation
    val standardOptions = NumericDistributionOptions(columnCount)
    val protoResult = calc.fun(standardOptions)(inputs)

    def checkResult(result: Traversable[(Option[String], Traversable[(BigDecimal, Int)])]) = {
      result.size should be (protoResult.size)

      result.toSeq.zip(protoResult.toSeq).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)
        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        counts1.toSeq.zip(counts2.toSeq).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (inputs.count(_._2.isDefined))
    }

    // streamed calculations
    val streamOptions = NumericDistributionFlowOptions(columnCount, inputs.flatMap(_._2).min, inputs.flatMap(_._2).max)
    calc.runFlow(streamOptions, streamOptions)(inputSource).map(checkResult)
  }
}