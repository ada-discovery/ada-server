package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.apache.commons.lang3.StringEscapeUtils
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.core.util.{seqFutures, writeStringAsStream}
import org.ada.server.services.StatsService
import org.ada.server.calc.impl.{ChiSquareResult, OneWayAnovaResult}

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RunIndependenceTestForDataSpace @Inject()(
    dsaf: DataSetAccessorFactory,
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
    statsService: StatsService
  ) extends InputFutureRunnableExt[RunIndependenceTestForDataSpaceSpec] {

  private val eol = "\n"
  private val headerColumnNames = Seq("dataSetId", "pValue", "fValue_or_statistics", "degreeOfFreedom", "testType")

  private val logger = Logger

  override def runAsFuture(
    input: RunIndependenceTestForDataSpaceSpec
  ) = {
    val unescapedDelimiter = StringEscapeUtils.unescapeJava(input.exportDelimiter)

    for {
      dataSpace <- dataSpaceMetaInfoRepo.get(input.dataSpaceId)

      dataSetIds = dataSpace.map(_.dataSetMetaInfos.map(_.id)).getOrElse(Nil)

      outputs <- seqFutures(dataSetIds)(
        runIndependenceTest(input.inputFieldName, input.targetFieldName, unescapedDelimiter)
      )
    } yield {
      val header = headerColumnNames.mkString(unescapedDelimiter)
      val output = (Seq(header) ++ outputs).mkString(eol)
      writeStringAsStream(output, new java.io.File(input.exportFileName))
    }
  }

  private def runIndependenceTest(
    inputFieldName: String,
    targetFieldName: String,
    delimiter: String)(
    dataSetId: String
  ): Future[String] = {
    logger.info(s"Running an independence test for the data set $dataSetId using the target field '$targetFieldName'.")
    val dsa = dsaf(dataSetId).get

    for {
      inputField <- dsa.fieldRepo.get(inputFieldName)
      targetField <- dsa.fieldRepo.get(targetFieldName)
      results <- statsService.testIndependence(dsa.dataSetRepo, Nil, Seq(inputField.get), targetField.get)
    } yield
      results.head.map(
        _ match {
          case x: ChiSquareResult => Seq(dataSetId, x.pValue, x.statistics, x.degreeOfFreedom, "Chi-Square").mkString(delimiter)
          case x: OneWayAnovaResult => Seq(dataSetId, x.pValue, x.FValue, x.dfwg, "ANOVA").mkString(delimiter)
        }
      ).getOrElse(
        Seq(dataSetId, "", "", "").mkString(delimiter)
      )
  }
}

case class RunIndependenceTestForDataSpaceSpec(
  dataSpaceId: BSONObjectID,
  inputFieldName: String,
  targetFieldName: String,
  exportFileName: String,
  exportDelimiter: String
)