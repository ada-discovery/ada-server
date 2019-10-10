package org.ada.server.services.importers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.fasterxml.jackson.core.JsonParseException
import com.google.inject.assistedinject.Assisted
import javax.inject.Inject
import org.ada.server.dataaccess.JsonUtil._
import org.ada.server.models.redcap.JsonFormat._
import org.ada.server.models.redcap._
import org.ada.server.services.{AdaRestException, AdaUnauthorizedAccessRestException}
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import play.api.Configuration
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsArray, JsObject}
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.ws.{WSClient, WSRequest, WSResponse}

import scala.concurrent.Future
import scala.concurrent.duration._

trait RedCapServiceFactory {
  def apply(@Assisted("url") url: String, @Assisted("token") token: String): RedCapService
}

trait RedCapService {

  /**
    * Retrieve all the records
    *
    * @return Records
    */
  def listAllRecords: Future[Seq[JsObject]]

  /**
    * List all the records for given events iteratively.
    *
    * @param events
    * @return
    */
  def listEventRecords(events: Seq[String]): Future[Seq[JsObject]]

  /**
    * Retrieve all metadata fields matching the filtering criterion and order them according to a reference field.
    *
    * @return Sorted records matching filter criterion.
    */
  def listMetadatas: Future[Seq[Metadata]]

  /**
    * Create list of all field names. Field names are sorted and filtered if they don't match filter criterion.
    *
    * @return Filtered and sorted list of records.
    */
  def listExportFields: Future[Seq[ExportField]]

  /**
    * Lists all the events (visits) associated with the study (token).
    *
    * @return
    */
  def listEvents: Future[Seq[Event]]

  /**
    * Count all records in reference field.
    *
    * @return The number of records matching the filter string.
    */
  def countRecords : Future[Int]

  /**
    * Get the name of the field specified by given id string.
    *
    * @param id String for matching with reference field.
    * @return Json representation of matching record.
    */
  def getRecord(id: String) : Future[Seq[JsObject]]

  /**
    * Retrieve the metadata matching the filter.
    *
    * @param id
    * @return
    */
  def getMetadata(id: String) : Future[Seq[JsObject]]

  /**
    * Get the name of the field(s) specified by given id string.
    *
    * @param id String for matching with reference field.
    * @return Sequence of field(s) matching id.
    */
  def getExportField(id: String) : Future[Seq[JsObject]]

  /**
    * Lock a given record
    *
    * @param record
    * @param event
    * @param instrument
    * @param instance
    */
  def lock(
    action: RedCapLockAction.Value,
    record: String,
    event: Option[String] = None,
    instrument: Option[String] = None,
    instance: Option[Int] = None,
    projectId: Option[Int] = None
  ): Future[Seq[LockRecordResponse]]
}

object RedCapLockAction extends Enumeration {
  val lock, unlock, status = Value
}

protected[services] class RedCapServiceWSImpl @Inject() (
    @Assisted("url") private val url: String,
    @Assisted("token") private val token: String,
    ws: WSClient,
    configuration: Configuration
  ) extends RedCapService {

  private val timeout = configuration.getLong("redcap.request.timeout").getOrElse(600000l)
  private val createUnsecuredClient = configuration.getBoolean("redcap.create_unsecured_client").getOrElse(false)

  private val wsClient: WSClient =
    if (createUnsecuredClient) {
      implicit val system = ActorSystem()
      implicit val materializer = ActorMaterializer()

      val config = new DefaultAsyncHttpClientConfig.Builder()
      config.setAcceptAnyCertificate(true)
      config.setFollowRedirect(true)
      config.setReadTimeout(timeout.toInt)
      // DefaultAsyncHttpClient(config.build
      AhcWSClient(config.build)

//      val config = new AsyncHttpClientConfigBean()
//      config.setAcceptAnyCertificate(true)
//      config.setFollowRedirect(true)
//      config.setReadTimeout(timeout.toInt)
//      new NingWSClient(config)
    } else
      ws

  private val req: WSRequest = wsClient.url(url).withRequestTimeout(timeout millis)

  private val baseRequestData = Map(
    "token" -> token,
    "format" -> "json"
  )

  private def recordRequest(events: Seq[String] = Nil) = {
    val eventsParam = if (events.nonEmpty) Map("events" -> events.mkString(",")) else Nil

    baseRequestData ++ Map("content" -> "record", "type" -> "flat") ++ eventsParam
  }

  private val metadataRequestData = baseRequestData ++ Map("content" -> "metadata")
  private val fieldNamesRequestData = baseRequestData ++ Map("content" -> "exportFieldNames")
  private val eventsRequestData = baseRequestData ++ Map("content" -> "event")

  // Services

  override def listAllRecords =
    runRedCapQuery(recordRequest())

  override def listEventRecords(events: Seq[String]) =
    runRedCapQuery(recordRequest(events))

  override def listMetadatas =
    runRedCapQuery(metadataRequestData).map(
      _.map(_.as[Metadata])
    )

  override def listExportFields =
    runRedCapQuery(fieldNamesRequestData).map(
      _.map(_.as[ExportField])
    )

  override def listEvents =
    runRedCapQuery(eventsRequestData).map(
      _.map(_.as[Event])
    )

  @Deprecated
  override def countRecords =
    runRedCapQuery(recordRequest()).map( items =>
      count(items, "", "")
    )

  @Deprecated
  override def getRecord(id: String) =
    runRedCapQuery(recordRequest()).map { items =>
      findBy(items, id, "cdisc_dm_usubjd")
    }

  @Deprecated
  override def getMetadata(id: String) =
    runRedCapQuery(metadataRequestData).map { items =>
      findBy(items, id, "field_name")
    }

  @Deprecated
  override def getExportField(id: String) =
    runRedCapQuery(fieldNamesRequestData).map { items =>
      findBy(items, id, "export_field_name")
    }

  private val lockingCustomErrorHandle: PartialFunction[WSResponse, Unit] = {
    case response if response.status == 400 =>
      val errorStart = response.body.indexOf("<error>")
      val errorEnd = response.body.indexOf("</error>")

      if (errorStart >= 0 && errorEnd >= 0) {
        val error = response.body.substring(errorStart + "<error>".size, errorEnd)
        throw new AdaRestException(error)
      } else
        throw new AdaRestException(response.status + ": " + response.statusText + "; " + response.body)
  }

  override def lock(
    action: RedCapLockAction.Value,
    record: String,
    event: Option[String],
    instrument: Option[String],
    instance: Option[Int],
    projectId: Option[Int]
  ): Future[Seq[LockRecordResponse]] = {
    // post data
    val requestData: Map[String, String] = Map(
      "token" -> token,
      "returnFormat" -> "json",
      "record" -> record
    ) ++ Seq(
      event.map("event" -> _),
      instrument.map("instrument" -> _),
      instance.map("instance" -> _.toString)
    ).flatten.toMap

    // query params
    val queryParams: Map[String, String] = Map(
      "NOAUTH" -> "",
      "type" -> "module",
      "prefix" -> "locking_api",
      "page" -> action.toString
    ) ++ Seq(projectId.map("pid" -> _.toString)).flatten.toMap

    runRedCapQuery(requestData, queryParams, Some(lockingCustomErrorHandle)).map(jsons =>
      jsons.map(_.as[LockRecordResponse])
    )
  }

  // Helper methods

  private def runRedCapQuery(
    requestData : Map[String, String],
    queryParams: Map[String, String] = Map(),
    customErrorHandle: Option[PartialFunction[WSResponse, Unit]] = None
  ) =
    req.withQueryString(queryParams.toSeq:_*).post(requestData.map { case (a, b) => (a, Seq(b)) }).map { response =>
      try {
        // handle error
        customErrorHandle.map( customHandle =>
          if (customHandle.isDefinedAt(response)) customHandle(response) else handleErrorResponse(response)
        ).getOrElse(
          handleErrorResponse(response)
        )

        // return response jsons
        response.json.asOpt[JsArray].map(
          _.value.asInstanceOf[Seq[JsObject]]
        ).getOrElse(
          throw new AdaRestException(s"JSON array response expected but got ${response.body}.")
        )
      } catch {
        case e: JsonParseException =>
          throw new AdaRestException(s"Couldn't parse a Red Cap JSON response due to ${e.getMessage}. Response body: ${response.body}.")
      }
    }

  private val handleErrorResponse: WSResponse => Unit = { response =>
    response.status match {
      case x if x >= 200 && x <= 299 => ()
      case 401 | 403 => throw new AdaUnauthorizedAccessRestException(response.status + ": Unauthorized access.")
      case _ => throw new AdaRestException(response.status + ": " + response.statusText + "; " + response.body)
    }
  }
}