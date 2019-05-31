package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.JsonUtil.jsonsToCsv
import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.apache.commons.lang3.StringEscapeUtils
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import reactivemongo.bson.BSONObjectID
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.core.util.{seqFutures, writeStringAsStream}

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class ExportDataSetsForSpaceAsCsvs @Inject() (
    dsaf: DataSetAccessorFactory,
    dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo
  ) extends InputFutureRunnableExt[ExportDataSetsForSpaceAsCsvsSpec] {

  private val eol = "\n"
  private val logger = Logger

  override def runAsFuture(
    input: ExportDataSetsForSpaceAsCsvsSpec
  ) =
    for {
      dataSpace <- dataSpaceMetaInfoRepo.get(input.dataSpaceId)

      dataSetIds = dataSpace.map(_.dataSetMetaInfos.map(_.id)).getOrElse(Nil)

      _ <- seqFutures(dataSetIds)(
        exportDataSet(input.delimiter, input.exportFolder)
      )
    } yield
      ()

  private def exportDataSet(
    delimiter: String,
    exportFolder: String)(
    dataSetId: String
  ) = {
    logger.info(s"Exporting the data set $dataSetId to the folder '$exportFolder'.")

    val dsa = dsaf(dataSetId).get

    for {
      jsons <- dsa.dataSetRepo.find()
      fields <- dsa.fieldRepo.find()
    } yield {
      val fieldNames = fields.map(_.name).toSeq.sorted
      val unescapedDelimiter = StringEscapeUtils.unescapeJava(delimiter)
      val unescapedEOL = StringEscapeUtils.unescapeJava(eol)
      val csvString = jsonsToCsv(jsons, unescapedDelimiter, unescapedEOL, fieldNames)

      writeStringAsStream(csvString, new java.io.File(exportFolder + "/" + dataSetId + ".csv"))
    }
  }
}

case class ExportDataSetsForSpaceAsCsvsSpec(
  dataSpaceId: BSONObjectID,
  delimiter: String,
  exportFolder: String
)
