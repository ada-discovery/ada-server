package stats

import org.scalatest._
import org.ada.server.services.{StatsService, GuicePlayTestApp}

import scala.util.Random

class EigenVectorsTest extends AsyncFlatSpec with Matchers with ExtraMatchers {

  private val inputs: Seq[Seq[Double]] = Seq(
    Seq(1, -1, 0),
    Seq(-1, 2, -1),
    Seq(0, -1, 1)
  )

  private val expectedEigenValues: Seq[Double] = Seq(3, 1, 0)
  private val expectedEigenVectors: Seq[Seq[Double]] = Seq(
    Seq(1, -2, 1),
    Seq(1, 0, -1),
    Seq(1, 1, 1)
  )

  private val randomInputSize = 100
  private val precision = 0.0001

  private val injector = GuicePlayTestApp().injector
  private val statsService = injector.instanceOf[StatsService]

  "Eigen vectors" should "match a static example" in {
    val (eigenValues, eigenVectors) = statsService.calcEigenValuesAndVectors(inputs)
    val (eigenValues2, eigenVectors2) = statsService.calcEigenValuesAndVectorsSymMatrixBreeze(inputs)
    val (eigenValues3, _, eigenVectors3) = statsService.calcEigenValuesAndVectorsBreeze(inputs)

    checkResult(eigenValues, eigenVectors, expectedEigenValues, expectedEigenVectors)
    checkResult(eigenValues2, eigenVectors2, expectedEigenValues, expectedEigenVectors)
    checkResult(eigenValues3, eigenVectors3, expectedEigenValues, expectedEigenVectors)
  }

  "Eigen vectors" should "match each other" in {
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

    val (eigenValues, eigenVectors) = statsService.calcEigenValuesAndVectors(symInputs)
    val (eigenValues2, eigenVectors2) = statsService.calcEigenValuesAndVectorsSymMatrixBreeze(symInputs)
    val (eigenValues3, _, eigenVectors3) = statsService.calcEigenValuesAndVectorsBreeze(symInputs)

    checkResult(eigenValues, eigenVectors, eigenValues2, eigenVectors2)
    checkResult(eigenValues, eigenVectors, eigenValues3, eigenVectors3)
  }

  def checkResult(
    eigenValues: Seq[Double],
    eigenVectors: Seq[Seq[Double]],
    eigenValues2: Seq[Double],
    eigenVectors2: Seq[Seq[Double]]
  ) = {
    eigenValues.zip(eigenValues2).map { case (value1, value2) =>
      value1 shouldBeAround (value2, precision)
    }

    (eigenVectors, eigenVectors2).zipped.map { case (eigenVector, eigenVector2) =>
      def transAux(vector: Seq[Double]) = {
        val head = vector.headOption.flatMap(value =>
          if (value != 0) Some(value) else None
        )

        head match {
          case Some(head) => vector.map(_ / head)
          case None => vector
        }
      }

      (transAux(eigenVector), transAux(eigenVector2)).zipped.map { case (value1, value2) =>
        value1 shouldBeAround (value2, precision)
      }

      eigenVector.size should be (eigenVector2.size)
    }

    eigenVectors.size should be (eigenVectors2.size)
  }
}