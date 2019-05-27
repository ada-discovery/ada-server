package stats

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.calc.impl.GroupUniqueDistributionCountsCalc
import org.ada.server.calc.CalculatorHelper._

import scala.concurrent.Future
import scala.util.Random

class GroupUniqueDistributionTest extends AsyncFlatSpec with Matchers {

  private val values1: Seq[(Option[String], Double)] = Seq(
    (Some("x"), 0.5),
    (Some("y"), 0.1),
    (Some("y"), 5),
    (Some("x"), 0.5),
    (Some("x"), 1),
    (Some("x"), 2),
    (None, 1),
    (Some("x"), 0.1),
    (Some("x"), 2),
    (Some("y"), 0.1),
    (Some("x"), 7),
    (None, 1),
    (Some("x"), 3),
    (Some("x"), 5),
    (Some("y"), 5),
    (Some("x"), 7),
    (Some("y"), 0.1),
    (Some("x"), 0.5),
    (None, 0.5),
    (Some("x"), 2),
    (Some("y"), 7)
  )
  private val expectedResult1 = Seq(
    None -> Seq(0.5 -> 1, 1.0 -> 2),
    Some("x") -> Seq(0.1 -> 1, 0.5 -> 3, 1.0 -> 1, 2.0 -> 3, 3.0 -> 1, 5.0 -> 1, 7.0 -> 2),
    Some("y") -> Seq(0.1 -> 3, 5.0 -> 2, 7.0 -> 1)
  )

  private val values2 = Seq(
    (Some("x"), None),
    (Some("x"), Some("cat")),
    (None -> Some("bird")),
    (Some("x"), None),
    (Some("x"), Some("dog")),
    (None, None),
    (Some("x"), Some("zebra")),
    (None -> Some("bird")),
    (Some("x"), Some("tiger")),
    (Some("x"), Some("dog")),
    (Some("x"), None),
    (None, None),
    (Some("y"), Some("tiger")),
    (None, Some("cat")),
    (Some("x"), Some("dolphin")),
    (Some("x"), Some("dolphin")),
    (None -> Some("bird")),
    (Some("x"), Some("cat")),
    (Some("y"), Some("tiger")),
    (Some("x"), Some("dolphin")),
    (Some("y"), None),
    (None, None)
  )

  private val expectedResult2 = Seq(
    None -> Seq(None -> 3, Some("bird") -> 3, Some("cat") -> 1),
    Some("x") -> Seq(None -> 3, Some("cat") -> 2, Some("dog") -> 2, Some("dolphin") -> 3, Some("tiger") -> 1, Some("zebra") -> 1),
    Some("y") -> Seq(None -> 1, Some("tiger") -> 2)
  )

  private val randomInputSize = 1000

  private val doubleCalc = GroupUniqueDistributionCountsCalc[String, Double]
  private val stringCalc = GroupUniqueDistributionCountsCalc[String, String]

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "Distributions" should "match the static example (double)" in {
    val inputs = values1.map{ case (group, value) => (group, Some(value)) }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkResult(result: Traversable[(Option[String], Traversable[(Option[Double], Int)])]) = {
      result.size should be (expectedResult1.size)

      val sorted = result.toSeq.sortBy(_._1)

      sorted.zip(expectedResult1).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)

        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        val sorted = counts1.toSeq.sortBy(_._1)
        sorted.zip(counts2).foreach { case ((Some(value1), count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (values1.size)
    }

    // standard calculation
    Future(doubleCalc.fun_(inputs)).map(checkResult)

    // streamed calculations
    doubleCalc.runFlow_(inputSource).map(checkResult)
  }

  "Distributions" should "match the static example (string)" in {
    val inputSource = Source.fromIterator(() => values2.toIterator)

    def checkResult(result: Traversable[(Option[String], Traversable[(Option[String], Int)])]) = {
      result.size should be (expectedResult2.size)

      val sorted = result.toSeq.sortBy(_._1)

      sorted.zip(expectedResult2).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)

        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        val sorted = counts1.toSeq.sortBy(_._1)
        sorted.zip(counts2).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (values2.size)
    }

    // standard calculation
    Future(stringCalc.fun_(values2)).map(checkResult)

    // streamed calculations
    stringCalc.runFlow_(inputSource).map(checkResult)
  }

  "Distributions" should "match each other (double)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toDouble)
      (group,  value)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = doubleCalc.fun_(inputs)

    def checkResult(result: Traversable[(Option[String], Traversable[(Option[Double], Int)])]) = {
      result.size should be (protoResult.size)

      result.toSeq.zip(protoResult.toSeq).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)

        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        val sorted1 = counts1.toSeq.sortBy(_._1)
        val sorted2 = counts2.toSeq.sortBy(_._1)

        sorted1.zip(sorted2).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (inputs.size)
    }

    // streamed calculations
    doubleCalc.runFlow_(inputSource).map(checkResult)
  }

  "Distributions" should "match each other (string)" in {
    val inputs = for (_ <- 1 to randomInputSize) yield {
      val group = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(4).toString)
      val value = if (Random.nextDouble() < 0.2) None else Some(Random.nextInt(20).toString)
      (group,  value)
    }
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    // standard calculation
    val protoResult = stringCalc.fun_(inputs)

    def checkResult(result: Traversable[(Option[String], Traversable[(Option[String], Int)])]) = {
      result.size should be (protoResult.size)

      result.toSeq.zip(protoResult.toSeq).foreach{ case ((groupName1, counts1), (groupName2, counts2)) =>
        groupName1 should be (groupName2)
        counts1.size should be (counts2.size)

        counts1.map(_._2).sum should be (counts2.map(_._2).sum)

        val sorted1 = counts1.toSeq.sortBy(_._1)
        val sorted2 = counts2.toSeq.sortBy(_._1)

        sorted1.zip(sorted2).foreach { case ((value1, count1), (value2, count2)) =>
          value1 should be (value2)
          count1 should be (count2)
        }
      }

      result.flatMap{ case (_, values) => values.map(_._2)}.sum should be (inputs.size)
    }

    // streamed calculations
    stringCalc.runFlow_(inputSource).map(checkResult)
  }
}