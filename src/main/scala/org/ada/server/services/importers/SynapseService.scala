package org.ada.server.services.importers

import akka.util.ByteString
import com.google.inject.assistedinject.Assisted
import javax.inject.Inject
import org.ada.server.models.synapse.JsonFormat._
import org.ada.server.models.synapse._
import org.ada.server.services.{AdaRestException, AdaUnauthorizedAccessRestException}
import org.incal.core.util.{ZipFileIterator, retry}
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.{WSClient, WSRequest, WSResponse}
import play.api.{Configuration, Logger}

import scala.concurrent.Await.result
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait SynapseServiceFactory {
  def apply(@Assisted("username") username: String, @Assisted("password") password: String): SynapseService
}

trait SynapseService {

  /**
    * (Re)logins and refreshes a session token
    */
  def login: Future[Unit]

  /**
    * Closes the connection
    *
    * @return
    */
  def close: Unit

  /**
    * Prolonges a current session token for another 24 hours
    */
  def prolongSession: Future[Unit]

  /**
    * .Runs a table query and returns a token
    */
  def startCsvTableQuery(tableId: String, sql: String): Future[String]

  /**
    * Gets results or a job status if still running
    */
  def getCsvTableResult(tableId: String, jobToken: String): Future[Either[DownloadFromTableResult, AsynchronousJobStatus]]

  /**
    * Gets results... wait till it's done by polling
    */
  def getCsvTableResultWait(tableId: String, jobToken: String): Future[DownloadFromTableResult]

  /**
    * Gets a file handle
    */
  def getFileHandle(fileHandleId: String): Future[FileHandle]

  /**
    * Downloads a file by following a URL for a given file handle id.
    * Only the person who created the FileHandle can download the file.
    * To download column files use downloadColumnFile
    *
    * @see http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/GET/fileHandle/handleId/url.html
    */
  def downloadFile(fileHandleId: String): Future[String]

  /**
    * Same as <code>downloadFile<code> but returns a byte array
    *
    * @param fileHandleId
    * @return
    */
  def downloadFileAsBytes(fileHandleId: String): Future[ByteString]

  /**
    * Gets a table as csv by combining runCsvTableQuery, getCsvTableResults, and downloadFile
    */
  def getTableAsCsv(tableId: String): Future[String]

  /**
    * Gets the column models of a given table
    */
  def getTableColumnModels(tableId: String): Future[PaginatedColumnModels]

  /**
    * Gets a list of file handles associated with the rows and columns specified in a row ref set
    */
  def getTableColumnFileHandles(rowReferenceSet: RowReferenceSet): Future[TableFileHandleResults]

  /**
    * Downloads a file associated with given column and row.
    */
  def downloadColumnFile(tableId: String, columnId: String, rowId: Int, versionNumber: Int): Future[String]

  /**
    * Initiates a downlaod of multiple filed... returns a token for polling
    *
    * @see http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/POST/file/bulk/async/start.html
    */
  def startBulkDownload(data: BulkFileDownloadRequest): Future[String]

  /**
    * Returns a file handle of the resulting bulk download zip file
    *
    * @see http://hud.rel.rest.doc.sagebase.org.s3-website-us-east-1.amazonaws.com/GET/file/bulk/async/get/asyncToken.html
    */
  def getBulkDownloadResult(jobToken: String): Future[Either[BulkFileDownloadResponse, AsynchronousJobStatus]]

  /**
    * Gets the bulk results... wait till it's done by polling
    */
  def getBulkDownloadResultWait(jobToken: String): Future[BulkFileDownloadResponse]

  /**
    * Downloads table files for given handle ids in a bulk by combining startBulkDownload, getBulkDownloadResultWait, and downloadFileAsBytes
    *
    * @param fileHandleIds
    * @param tableId
    * @return
    */
  def downloadTableFilesInBulk(
    fileHandleIds: Traversable[String],
    tableId: String,
    attemptNum: Option[Int] = None
  ): Future[Iterator[(String, String)]]

  /**
    * Downloads files of custom types (file, table, attachment) for given handle assocations in a bulk by combining startBulkDownload, getBulkDownloadResultWait, and downloadFileAsBytes
    *
    * @param fileHandleAssociations
    * @param attemptNum
    * @return
    */
  def downloadFilesInBulk(
    fileHandleAssociations: Traversable[FileHandleAssociation],
    attemptNum: Option[Int] = None
  ): Future[Iterator[(String, String)]]
}

protected[services] class SynapseServiceWSImpl @Inject() (
    @Assisted("username") private val username: String,
    @Assisted("password") private val password: String,
    ws: WSClient,
    configuration: Configuration
  ) extends SynapseService {

  private val baseUrl = configuration.getString("synapse.api.rest.url").get
  private val loginSubUrl = configuration.getString("synapse.api.login.url").get
  private val prolongSessionUrl = configuration.getString("synapse.api.session.url").get
  private val tableCsvDownloadStartSubUrl1 = configuration.getString("synapse.api.table_csv_download_start.url.part1").get
  private val tableCsvDownloadStartSubUrl2 = configuration.getString("synapse.api.table_csv_download_start.url.part2").get
  private val tableCsvDownloadResultSubUrl1 = configuration.getString("synapse.api.table_csv_download_result.url.part1").get
  private val tableCsvDownloadResultSubUrl2 = configuration.getString("synapse.api.table_csv_download_result.url.part2").get
  private val fileHandleSubUrl = configuration.getString("synapse.api.file_handle.url").get
  private val fileDownloadSubUrl1 = configuration.getString("synapse.api.file_download.url.part1").get
  private val fileDownloadSubUrl2 = configuration.getString("synapse.api.file_download.url.part2").get
  private val columnModelsSubUrl1 = configuration.getString("synapse.api.table_column_models.url.part1").get
  private val columnModelsSubUrl2 = configuration.getString("synapse.api.table_column_models.url.part2").get
  private val columnFileHandleSubUrl1 = configuration.getString("synapse.api.column_file_handles.url.part1").get
  private val columnFileHandleSubUrl2 = configuration.getString("synapse.api.column_file_handles.url.part2").get
  private val fileColumnDownloadSubUrl1 = configuration.getString("synapse.api.file_column_download.url.part1").get
  private val fileColumnDownloadSubUrl2 = configuration.getString("synapse.api.file_column_download.url.part2").get
  private val fileColumnDownloadSubUrl3 = configuration.getString("synapse.api.file_column_download.url.part3").get
  private val fileColumnDownloadSubUrl4 = configuration.getString("synapse.api.file_column_download.url.part4").get
  private val fileColumnDownloadSubUrl5 = configuration.getString("synapse.api.file_column_download.url.part5").get
  private val bulkDownloadStartUrl = configuration.getString("synapse.api.bulk_download_start.url").get
  private val bulkDownloadResultUrl = configuration.getString("synapse.api.bulk_download_result.url").get

  private val logger = Logger

  private val timeout = 10 minutes
  private val tableCsvResultPollingFreq = 200
  private val bulkDownloadResultPollingFreq = 400
  private var sessionToken: Option[String] = None

  override def login: Future[Unit] = {
    val request = withJsonContent(
        ws.url(baseUrl + loginSubUrl))

    val data = Json.obj("username" -> username, "password" -> password)
    request.post(data).map { response =>
      val newSessionToken = (response.json.as[JsObject] \ "sessionToken").get.as[String]
      sessionToken = Some(newSessionToken)
    }
  }

  override def prolongSession: Future[Unit] = {
    val request = ws.url(baseUrl + prolongSessionUrl)

    request.put(SessionFormat.writes(Session(getSessionToken, true))).map { response =>
      handleErrorResponse(response)
    }
  }

  override def startCsvTableQuery(tableId: String, sql: String): Future[String] = {
    val request = withJsonContent(withSessionToken(
        ws.url(baseUrl + tableCsvDownloadStartSubUrl1 + tableId + tableCsvDownloadStartSubUrl2)))

    val data = Json.obj("sql" -> sql)
    request.post(data).map { response =>
      handleErrorResponse(response)
      (response.json.as[JsObject] \ "token").get.as[String]
    }
  }

  override def getCsvTableResult(tableId: String, jobToken: String): Future[Either[DownloadFromTableResult, AsynchronousJobStatus]] = {
    val request = withSessionToken(
        ws.url(baseUrl + tableCsvDownloadResultSubUrl1 + tableId + tableCsvDownloadResultSubUrl2 + jobToken))

    request.get.map { response =>
      handleErrorResponse(response)
      val json = response.json
      response.status match {
        case 201 => Left(json.as[DownloadFromTableResult])
        case 202 => Right(json.as[AsynchronousJobStatus])
      }
    }
  }

  override def getCsvTableResultWait(tableId: String, jobToken: String): Future[DownloadFromTableResult] = Future {
    var res: Either[DownloadFromTableResult, AsynchronousJobStatus] = null
    while ({res = result(getCsvTableResult(tableId, jobToken), timeout); res.isRight}) {
      Thread.sleep(tableCsvResultPollingFreq)
    }
    res.left.get
  }

  override def getFileHandle(fileHandleId: String): Future[FileHandle] = {
    val request = withSessionToken(
        ws.url(baseUrl + fileHandleSubUrl + fileHandleId))

    request.get.map { response =>
      handleErrorResponse(response)
      response.json.as[FileHandle]
    }
  }

  override def downloadFile(fileHandleId: String): Future[String] = {
    val request = withSessionToken(
        ws.url(baseUrl + fileDownloadSubUrl1 + fileHandleId + fileDownloadSubUrl2)).withFollowRedirects(true)

    request.get.map { response =>
      handleErrorResponse(response)
      response.body
    }
  }

  override def downloadFileAsBytes(fileHandleId: String): Future[ByteString] = {
    val request = withSessionToken(
      ws.url(baseUrl + fileDownloadSubUrl1 + fileHandleId + fileDownloadSubUrl2)).withFollowRedirects(true)

    request.get.map { response =>
      handleErrorResponse(response)
      response.bodyAsBytes
    }
  }

  override def getTableAsCsv(tableId: String): Future[String] =
    for {
      jobToken <- startCsvTableQuery(tableId, s"SELECT * FROM $tableId")
      result <- getCsvTableResultWait(tableId, jobToken)
      content <- downloadFile(result.resultsFileHandleId)
    } yield
      content

  override def getTableColumnFileHandles(rowReferenceSet: RowReferenceSet): Future[TableFileHandleResults] = {
    val request = withJsonContent(withSessionToken(
        ws.url(baseUrl + columnFileHandleSubUrl1 + rowReferenceSet.tableId + columnFileHandleSubUrl2)))

    request.post(Json.toJson(rowReferenceSet)).map { response =>
      handleErrorResponse(response)
      response.json.as[TableFileHandleResults]
    }
  }

  override def downloadColumnFile(tableId: String, columnId: String, rowId: Int, versionNumber: Int): Future[String] = {
    val request = withSessionToken(
        ws.url(baseUrl + fileColumnDownloadSubUrl1 + tableId + fileColumnDownloadSubUrl2 + columnId +
          fileColumnDownloadSubUrl3 + rowId + fileColumnDownloadSubUrl4 + versionNumber + fileColumnDownloadSubUrl5))

    request.withFollowRedirects(true).get.map { response =>
      handleErrorResponse(response)
      response.body
    }
  }

  private def handleErrorResponse(response: WSResponse): Unit =
    response.status match {
      case x if x >= 200 && x<= 299 => ()
      case 401 => throw new AdaUnauthorizedAccessRestException(response.statusText)
      case _ => throw new AdaRestException(response.status + ": " + response.statusText + "; " + response.body)
    }

  // could be used to automatically reauthorize...
  private def accessRetry[T](action: => T): T =
    try {
      action
    } catch {
      case e: AdaUnauthorizedAccessRestException => { login; action }
    }

  private def getSessionToken = synchronized {
    if (sessionToken.isEmpty) result(login, timeout)
    sessionToken.get
  }

  override def getTableColumnModels(tableId: String): Future[PaginatedColumnModels] = {
    val getColumnModelsReq = withSessionToken(
        ws.url(baseUrl + columnModelsSubUrl1 + tableId + columnModelsSubUrl2)
      )

    getColumnModelsReq.get.map { response =>
      handleErrorResponse(response)
      response.json.as[PaginatedColumnModels]
    }
  }

  override def startBulkDownload(data: BulkFileDownloadRequest): Future[String] = {
    val request = withJsonContent(withSessionToken(
      ws.url(baseUrl + bulkDownloadStartUrl)))

    request.post(Json.toJson(data)).map { response =>
      handleErrorResponse(response)
      (response.json.as[JsObject] \ "token").get.as[String]
    }
  }

  override def getBulkDownloadResult(jobToken: String): Future[Either[BulkFileDownloadResponse, AsynchronousJobStatus]] = {
    val request =
      withRequestTimeout(timeout)(
        withSessionToken(
          ws.url(baseUrl + bulkDownloadResultUrl + jobToken)))

    request.get.map { response =>
      handleErrorResponse(response)
      val json = response.json
      response.status match {
        case 201 => Left(json.as[BulkFileDownloadResponse])
        case 202 => Right(json.as[AsynchronousJobStatus])
      }
    }
  }

  override def getBulkDownloadResultWait(jobToken: String): Future[BulkFileDownloadResponse] = Future {
    var res: Either[BulkFileDownloadResponse, AsynchronousJobStatus] = null
    while ({res = result(getBulkDownloadResult(jobToken), timeout); res.isRight}) {
      Thread.sleep(bulkDownloadResultPollingFreq)
    }
    res.left.get
  }

  override def downloadTableFilesInBulk(
    fileHandleIds: Traversable[String],
    tableId: String,
    attemptNum: Option[Int]
  ): Future[Iterator[(String, String)]] = {
    val assocs = fileHandleIds.map(FileHandleAssociation(FileHandleAssociateType.TableEntity, _, tableId))
    downloadFilesInBulk(assocs,  attemptNum)
  }

  override def downloadFilesInBulk(
    fileHandleAssociations: Traversable[FileHandleAssociation],
    attemptNum: Option[Int]
  ): Future[Iterator[(String, String)]] =
    if (fileHandleAssociations.isEmpty) {
      Future(Iterator[(String, String)]())
    } else {
      def downloadAux = for {
        bulkDownloadToken <- {
          startBulkDownload(BulkFileDownloadRequest(fileHandleAssociations.toSeq))
        }
        bulkDownloadResponse <- getBulkDownloadResultWait(bulkDownloadToken)
        bulkDownloadContentBytes <- downloadFileAsBytes(bulkDownloadResponse.resultZipFileHandleId)
      } yield {
        val fileNameHandleIdMap = bulkDownloadResponse.fileSummary.map { fileSummary =>
          fileSummary.failureCode.map { failureCode =>
            val fileHandleIds = fileHandleAssociations.map(_.fileHandleId).mkString(", ")
            val failureMessage = fileSummary.failureMessage.getOrElse("")
            throw new AdaRestException(s"Synapse bulk download of $fileHandleIds failed due to: $failureCode; $failureMessage.")
          }.getOrElse(
            (fileSummary.zipEntryName.get, fileSummary.fileHandleId)
          )
        }.toMap
        ZipFileIterator(bulkDownloadContentBytes.toStream.toArray).map(pair => (fileNameHandleIdMap.get(pair._1).get, pair._2))
      }

      attemptNum match {
        case Some(attemptNum) => retry("Synapse bulk download failed:", logger.warn(_), attemptNum)(downloadAux)
        case None => downloadAux
      }
    }

  def withSessionToken(request: WSRequest): WSRequest =
    request.withHeaders("sessionToken" -> getSessionToken)

  def withJsonContent(request: WSRequest): WSRequest =
    request.withHeaders("Content-Type" -> "application/json")

  def withRequestTimeout(timeout: Duration)(request: WSRequest): WSRequest =
    request.withRequestTimeout(timeout)

  /**
    * Closes the connection
    *
    * @return
    */
  override def close = ws.close()
}