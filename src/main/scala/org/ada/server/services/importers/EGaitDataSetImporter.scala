package org.ada.server.services.importers

import java.util.Date

import javax.inject.Inject
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models._
import play.api.Configuration
import play.api.libs.json.{JsObject, Json}
import org.ada.server.services.importers.EGaitServiceFactory
import org.ada.server.models.egait.EGaitKineticData.eGaitSessionFormat
import org.ada.server.models.egait.EGaitKineticData
import org.ada.server.models.dataimport.EGaitDataSetImport
import org.ada.server.AdaException
import org.ada.server.dataaccess.dataset.DataSetAccessor

import scala.concurrent.Await._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class EGaitDataSetImporter @Inject()(
    eGaitServiceFactory: EGaitServiceFactory,
    configuration: Configuration
  ) extends AbstractDataSetImporter[EGaitDataSetImport] {

  private val delimiter = ','
  private val eol = "\r\n"

  private lazy val username = confValue("egait.api.username")
  private lazy val password = confValue("egait.api.password")
  private lazy val certificateFileName = confValue("egait.api.certificate.path")
  private lazy val baseUrl = confValue("egait.api.rest.url")

  private def confValue(key: String) = configuration.getString(key).getOrElse(
    throw new AdaException(s"Configuration entry '$key' not specified.")
  )

  private val saveBatchSize = 100
//  private val rawSaveBatchSize = 2

  private val prefixSuffixSeparators = Seq(
    ("\"", "\""),
    ("[", "]")
  )

  // Field type inferrer
  private val fti = {
    val ftf = FieldTypeHelper.fieldTypeFactory(nullAliases = Set("", "-"))
    FieldTypeHelper.fieldTypeInferrerFactory(ftf).apply
  }

  private val rawKineticDataDictionary = Seq(
    Field("sessionId", None, FieldTypeId.String),
    Field("personId", None, FieldTypeId.String),
    Field("instructor", None, FieldTypeId.String),
    Field("startTime", None, FieldTypeId.Date),
    Field("testName", None, FieldTypeId.String),
    Field("testDuration", None, FieldTypeId.Integer),
    Field("rightSensorFileName", None, FieldTypeId.String),
    Field("leftSensorFileName", None, FieldTypeId.String),
    Field("rightSensorStartIndex", None, FieldTypeId.Integer),
    Field("rightSensorStopIndex", None, FieldTypeId.Integer),
    Field("leftSensorStartIndex", None, FieldTypeId.Integer),
    Field("leftSensorStopIndex", None, FieldTypeId.Integer),
    Field("rightAccelerometerPoints", None, FieldTypeId.Json, true),
    Field("rightGyroscopePoints", None, FieldTypeId.Json, true),
    Field("leftAccelerometerPoints", None, FieldTypeId.Json, true),
    Field("leftGyroscopePoints", None, FieldTypeId.Json, true)
  )

  override def apply(importInfo: EGaitDataSetImport): Future[Unit] = {
    logger.info(new Date().toString)
    logger.info(s"Import of data set '${importInfo.dataSetName}' initiated.")

    try {
      val eGaitService = eGaitServiceFactory(username, password, baseUrl)
      for {
        dsa <- createDataSetAccessor(importInfo)

        _ <- if (importInfo.importRawData)
          importRawKineticData(importInfo, eGaitService, dsa)
        else
          importFeatures(importInfo, eGaitService, dsa)

      } yield {
        messageLogger.info(s"Import of data set '${importInfo.dataSetName}' successfully finished.")
      }
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  private def importFeatures(
    importInfo: EGaitDataSetImport,
    eGaitService: EGaitService,
    dsa: DataSetAccessor
  ): Future[Unit] =
    for {
      csvs <- {
        logger.info("Downloading CSV table from eGait...")
        getSessionCsvs(eGaitService)
      }

      lines: Iterator[String] =
        csvs.map(_.split(eol)) match {
          case Nil => Seq[String]().iterator
          case csvLines =>
            // skip the header from all but the first csv
            val tailLines = csvLines.tail.map(_.tail).flatten
            val all = csvLines.head ++ tailLines
//            println(all.mkString("\n"))
            all.iterator
          }

      // collect the column names and labels
      columnNameLabels =  dataSetService.getColumnNameLabels(delimiter.toString, lines)

      // parse lines
      values = {
        logger.info(s"Parsing lines...")
        dataSetService.parseLines(columnNameLabels.size, lines, delimiter.toString, true, prefixSuffixSeparators)
      }

      _ <- saveStringsAndDictionaryWithTypeInference(dsa, columnNameLabels, values, Some(saveBatchSize), Some(fti))
    } yield
      ()

  def importRawKineticData(
    importInfo: EGaitDataSetImport,
    eGaitService: EGaitService,
    dsa: DataSetAccessor
  ): Future[Unit] =
    for {
      // retrieve the raw kinetic data
      kineticDatas <- {
        logger.info("Downloading raw kinetic data from eGait...")
        getRawSessionKineticData(eGaitService)
      }

      // create jsons
      jsons = kineticDatas.map(Json.toJson(_).as[JsObject]).toSeq

      // save jsons and the dictionary
      _ <- saveJsonsAndDictionary(dsa, jsons, rawKineticDataDictionary, None)
    } yield
      ()

  private def getSessionCsvs(
    eGaitService: EGaitService
  ): Future[Traversable[String]] =
    for {
      proxySessionToken <- eGaitService.getProxySessionToken(certificateFileName)

      userSessionId <- eGaitService.login(proxySessionToken)

      searchSessionIds <- eGaitService.searchSessions(proxySessionToken, userSessionId)

      csvs <- Future.sequence(
        searchSessionIds.map( searchSessionId =>
          eGaitService.downloadParametersAsCSV(proxySessionToken, userSessionId, searchSessionId)
        )
      )

      _ <- eGaitService.logoff(proxySessionToken, userSessionId)
    } yield
      csvs

  private def getRawSessionKineticData(
    eGaitService: EGaitService
  ): Future[Traversable[EGaitKineticData]] =
    for {
      proxySessionToken <- eGaitService.getProxySessionToken(certificateFileName)

      userSessionId <- eGaitService.login(proxySessionToken)

      searchSessionIds <- eGaitService.searchSessions(proxySessionToken, userSessionId)

      kineticDatas <- Future.sequence(
        searchSessionIds.map( searchSessionId =>
          eGaitService.downloadRawDataStructured(proxySessionToken, userSessionId, searchSessionId)
        )
      )

      _ <- eGaitService.logoff(proxySessionToken, userSessionId)
    } yield
      kineticDatas.flatten
}