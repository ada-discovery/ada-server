package stats

import akka.stream.scaladsl.Source
import org.scalatest._
import org.ada.server.services.{StatsService, GuicePlayTestApp}

import scala.concurrent.Future
import scala.util.Random

class MetricMDSTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val inputs: Seq[Seq[Double]] =
    Seq(
      Seq(0, 2, 3, 4, 5, 5),
      Seq(2, 0, 3, 5, 6, 5),
      Seq(3, 3, 0, 5, 4, 6),
      Seq(4, 5, 5, 0, 4, 3),
      Seq(5, 6, 4, 4, 0, 2),
      Seq(5, 5, 6, 3, 2, 0)
    )

  private val expectedResult = Seq(
    Seq(-1.9301, -0.6756, 0.3818, 1.0441),
    Seq(-2.6179, -1.1281, -1.1303, -0.4680),
    Seq(-2.1119, 2.0914, 0.4168, -0.4032),
    Seq(1.4786, -1.3608, 1.8070, -0.3940),
    Seq(2.3836, 2.0059, -0.2743, 0.2351),
    Seq(2.7976, -0.9328, -1.2011, -0.0140)
  )

  private val randomInputSize = 1000
  private val precision = 0.001

  private val injector = GuicePlayTestApp().injector
  private val statsService = injector.instanceOf[StatsService]

  "Metric MDS" should "match the static example" in {
    val inputSource = Source.fromIterator(() => inputs.toIterator)

    def checkAux(
      calc: Int => Future[(Seq[Seq[Double]], Seq[Double])],
      dims: Int
    ): Future[Assertion] =
      calc(dims).map { case (mdsProjections, _) =>
        checkResult(expectedResult, dims)(mdsProjections)
      }

    // standard calculation
    def checkStandardAux(dims: Int) = checkAux(
      (dims: Int) => statsService.performMetricMDS(inputs, dims, true), dims
    )

    // streamed calculation
    def checkStreamedAux(dims: Int) = checkAux(
      statsService.performMetricMDS(inputSource, _, true), dims
    )

    // dims: 2
    checkStandardAux(2)
    checkStreamedAux(2)

    // dims: 3
    checkStandardAux(3)
    checkStreamedAux(3)

    // dims: 4
    checkStandardAux(4)
    checkStreamedAux(4)
  }

  "Metric MDS" should "match each other" in {
    val inputs =
      for (i <- 0 to randomInputSize - 1) yield {
        for (j <- 0 to randomInputSize - 1) yield
          if (i != j) Random.nextDouble() else 0d
      }

    val symInputs =
      for (i <- 0 to randomInputSize - 1) yield {
        for (j <- 0 to randomInputSize - 1) yield
          if (i == j) 0 else if (i < j) inputs(i)(j) else inputs(j)(i)
      }

    val inputSource = Source.fromIterator(() => symInputs.toIterator)

    def checkAux(dims: Int): Future[Assertion] = {
      for {
        (expectedMdsProjections, _) <- statsService.performMetricMDS(symInputs, dims, false)
        (mdsProjections, _) <- statsService.performMetricMDS(inputSource, dims, false)
      } yield
        checkResult(expectedMdsProjections, dims)(mdsProjections)
    }

    // dims: 2
    checkAux(2)

    // dims: 3
    checkAux(3)

    // dims: 4
    checkAux(4)
  }

  def checkResult(
    expectedMdsProjections: Seq[Seq[Double]],
    dims: Int)(
    mdsProjections: Seq[Seq[Double]]
  ) = {
    def transAux(vector: Seq[Double]) = {
      val head = vector.headOption.flatMap(value =>
        if (value != 0) Some(value) else None
      )

      head match {
        case Some(head) => vector.map(_ / head)
        case None => vector
      }
    }

    val expectedMdsProjectionsWithOne = expectedMdsProjections.transpose.map(transAux).transpose
    val mdsProjectionsWithOne = mdsProjections.transpose.map(transAux).transpose

    (mdsProjectionsWithOne, expectedMdsProjectionsWithOne).zipped.map { case (projection, expectedProjection) =>
      val trimmedExpProjection = expectedProjection.take(dims)

      (projection, trimmedExpProjection).zipped.map { case (value, expValue) =>
        value shouldBeAround (expValue, precision)
      }

      projection.size should be (trimmedExpProjection.size)
    }

    mdsProjections.size should be (expectedMdsProjections.size)
  }
}