package org.ada.server.runnables.core

import java.nio.charset.StandardCharsets
import javax.inject.Inject

import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.incal.core.dataaccess.Criterion._
import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.apache.commons.lang3.StringEscapeUtils
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.services.StatsService
import org.incal.core.util.writeStringAsStream

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RunIndependenceTestForMultipleTargets @Inject()(
    dsaf: DataSetAccessorFactory,
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
    statsService: StatsService
  ) extends InputFutureRunnableExt[RunIndependenceTestForMultipleTargetsSpec] {

  private val eol = "\n"
  private val logger = Logger

  override def runAsFuture(
    input: RunIndependenceTestForMultipleTargetsSpec
  ) =
    runIndependenceTests(input).map { outputs =>
      val unescapedDelimiter = StringEscapeUtils.unescapeJava(input.exportDelimiter)

      val header = (Seq("targetFieldName") ++ input.inputFieldNames.sorted).mkString(unescapedDelimiter)
      val output = (Seq(header) ++ outputs).mkString(eol)

      writeStringAsStream(output, new java.io.File(input.exportFileName))
    }

  private def runIndependenceTests(
    input: RunIndependenceTestForMultipleTargetsSpec
  ): Future[Traversable[String]] = {
    logger.info(s"Running independence tests for the data set ${input.dataSetId} using the target fields '${input.targetFieldNames.mkString(", ")}'.")
    val dsa = dsaf(input.dataSetId).get
    val unescapedDelimiter = StringEscapeUtils.unescapeJava(input.exportDelimiter)

    for {
      jsons <- dsa.dataSetRepo.find(projection = input.inputFieldNames ++ input.targetFieldNames)
      inputFields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> input.inputFieldNames))
      targetFields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> input.targetFieldNames))
    } yield {
      val inputFieldsSeq = inputFields.toSeq.sortBy(_.name)

      targetFields.map { targetField =>
        val pValues = statsService.testIndependenceSortedJson(jsons, inputFieldsSeq, targetField).flatMap(_._2.map(_.pValue))

        (Seq(targetField.name) ++ pValues).mkString(unescapedDelimiter)
      }
    }
  }
}

case class RunIndependenceTestForMultipleTargetsSpec(
  dataSetId: String,
  inputFieldNames: Seq[String],
  targetFieldNames: Seq[String],
  exportFileName: String,
  exportDelimiter: String
)