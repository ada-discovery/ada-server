package org.ada.server.services.importers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.google.inject.assistedinject.Assisted
import javax.inject.Inject
import org.ada.server.dataaccess.ConversionUtil
import org.ada.server.models.egait.{EGaitKineticData, SpatialPoint}
import org.apache.commons.codec.binary.Base64
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.incal.core.util.ZipFileIterator
import play.api.libs.ws._
import play.api.libs.ws.ahc.AhcWSClient
import play.api.{Configuration, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.xml.{Node, XML}

trait EGaitServiceFactory {
  def apply(
    @Assisted("username") username: String,
    @Assisted("password") password: String,
    @Assisted("baseUrl") baseUrl: String
  ): EGaitService
}

trait EGaitService {

  /**
    * Gets a session token needed for login and all services
    */
  def getProxySessionToken(certificateFileName: String): Future[String]

  /**
    * Gets a connection token used to access a given service
    *
    * @param serviceName
    * @return
    */
  def getConnectionToken(
    serviceName: String,
    sessionToken: String
  ): Future[String]

  /**
    * Logins and returns a user session id
    */
  def login(sessionToken: String): Future[String]

  /**
    * Logoffs
    *
    * @param sessionToken
    * @param userSessionId
    * @return
    */
  def logoff(sessionToken: String, userSessionId: String): Future[Unit]

  /**
    *
    * @param sessionToken
    * @param userSessionId
    */
  def searchSessions(
    sessionToken: String,
    userSessionId: String
  ): Future[Traversable[String]]

  def downloadParametersAsCSV(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[String]

  def downloadRawData(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[ByteString]

  def downloadRawDataStructured(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[Seq[EGaitKineticData]]
}

protected[services] class EGaitServiceWSImpl @Inject() (
    @Assisted("username") private val username: String,
    @Assisted("password") private val password: String,
    @Assisted("baseUrl") private val baseUrl: String,
  //    ws: WSClient,
    configuration: Configuration
  ) extends EGaitService {

  object Url {
    val Session = confValue("egait.api.session.url")
    val ServiceConnectionToken = confValue("egait.api.service_connection_token.url")
    val Login = confValue("egait.api.login.url")
    val Logoff = confValue("egait.api.logoff.url")
    val SearchSessions = confValue("egait.api.search_sessions.url")
    val DownloadParametersAsCsvSub1 = confValue("egait.api.download_parameters_as_csv.url.part1")
    val DownloadParametersAsCsvSub2 = confValue("egait.api.download_parameters_as_csv.url.part2")
    val DownloadRawData = confValue("egait.api.download_raw_data.url")
  }

  object ConnectionServiceName {
    val Authentication = "AuthenticationService"
    val SearchSessions = "MilifeSession"
    val CsvDownload = "MilifeRest"
  }

  private val dateFormat = "yyyy-MM-dd'T'HH:mm:ss"

  private val ws = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val config = new DefaultAsyncHttpClientConfig.Builder()
    config.setAcceptAnyCertificate(true)
    config.setFollowRedirect(true)
    // DefaultAsyncHttpClient(config.build
    AhcWSClient(config.build)

//    val config = new AsyncHttpClientConfigBean
//    config.setAcceptAnyCertificate(true)
//    config.setFollowRedirect(true)
//    NingWSClient(config)
  }

  private val logger = Logger

  private def confValue(key: String) = configuration.getString(key).get

  override def getProxySessionToken(certificateFileName: String): Future[String] = {
    val certificateContent = loadFileAsBase64(certificateFileName)

    val request = ws.url(baseUrl + Url.Session).withHeaders(
      "Content-Type" -> "application/octet-stream",
      "Content-Length" -> certificateContent.length.toString
    )

    request.post(certificateContent).map(
      _.header("session-token").get
    )
  }

  override def getConnectionToken(
    serviceName: String,
    sessionToken: String
  ): Future[String] = {
    val request = ws.url(baseUrl + Url.ServiceConnectionToken + serviceName).withHeaders(
      "session-token" -> sessionToken
    )

    request.get.map(
      _.header("connect-token").get
    )
  }

  override def login(sessionToken: String): Future[String] = {
    val loginInfoXML =
      s"""
        <LoginWithClientInfo xmlns="http://tempuri.org/">
        <userName>${username}</userName>
        <passwordHash>${password}</passwordHash>
        <clientData xmlns:a="http://schemas.datacontract.org/2004/07/AstrumIT.Meditalk.Platform.Core.Interfaces.Security" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        <a:ClientId>5c11a22b-fd41-41eb-ad8b-af307a3bfb88</a:ClientId>
        <a:ClientName>Dres med Maria von Witwenkind (Erlangen)</a:ClientName>
        <a:CustomData></a:CustomData>
        <a:WindowsUser>dkpeters</a:WindowsUser></clientData></LoginWithClientInfo>
      """

    for {
      // get the connection token for the authentication service
      connectionToken <-
        getConnectionToken(ConnectionServiceName.Authentication, sessionToken)

      // login and obtain a user session id
      userSessionId <- {
        val request =
          withXmlContent(
            withConnectionToken(connectionToken)(
              ws.url(baseUrl + Url.Login)
            )
          )

        request.post(loginInfoXML).map { response =>
          (response.xml \\ "SessionId").text
        }
      }
    } yield
      userSessionId
  }

  override def logoff(
    sessionToken: String,
    userSessionId: String
  ): Future[Unit] = {
    val logoffInfoXML = s"""<Logoff xmlns="http://tempuri.org/s"><sessionId>${userSessionId}</sessionId></Logoff>"""

    for {
      // get the connection token for the authentication service
      connectionToken <-
        getConnectionToken(ConnectionServiceName.Authentication, sessionToken)

      // logs off
      _ <- {
        val request =
          withXmlContent(
            withConnectionToken(connectionToken)(
              withUserSessionId(userSessionId)(
                ws.url(baseUrl + Url.Logoff)
              )
            )
          )

        request.post(logoffInfoXML).map(_ => ws.close())
      }
    } yield ()
  }

  override def searchSessions(
    sessionToken: String,
    userSessionId: String
  ): Future[Traversable[String]] = {
    val findAllFilterXML =
       """
          <Session xmlns="http://schemas.datacontract.org/2004/07/AstrumIT.MiLife.Server.MiLifeWcfRestServiceInterface.DataModel.SearchSession" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
          </Session>
       """

    for {
      // get the connection token for the authentication service
      connectionToken <-
        getConnectionToken(ConnectionServiceName.SearchSessions, sessionToken)

      // search sessions and obtain the ids
      searchSessionIds <- {
        val request =
          withXmlContent(
            withConnectionToken(connectionToken)(
              withUserSessionId(userSessionId)(
                ws.url(baseUrl + Url.SearchSessions)
              )
            )
          )

        request.post(findAllFilterXML).map( response =>
          (response.xml \\ "Session" \ "SessionId").map(_.text)
        )
      }
    } yield
      searchSessionIds
  }

  override def downloadParametersAsCSV(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[String] =
    for {
      // get the connection token for the authentication service
      connectionToken <-
        getConnectionToken(ConnectionServiceName.CsvDownload, sessionToken)

      // get the csv (content)
      csv <- {
        val request =
          withConnectionToken(connectionToken)(
            withUserSessionId(userSessionId)(
              ws.url(baseUrl + Url.DownloadParametersAsCsvSub1 + searchSessionId + Url.DownloadParametersAsCsvSub2)
            )
          )

        request.get.map(_.body)
      }
    } yield
      csv

  override def downloadRawData(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[ByteString] =
    for {
      // get the connection token for the authentication service
      connectionToken <-
        getConnectionToken(ConnectionServiceName.CsvDownload, sessionToken)

      // get the csv (content)
      bytes <- {
        val request =
          withConnectionToken(connectionToken)(
            withUserSessionId(userSessionId)(
              ws.url(baseUrl + Url.DownloadRawData + searchSessionId)
            )
          )

        request.get.map(_.bodyAsBytes)
      }
    } yield
      bytes

  case class TestInfo(name: String, duration: Int, rightStart: Int, rightStop: Int, leftStart: Int, leftStop: Int)

  override def downloadRawDataStructured(
    sessionToken: String,
    userSessionId: String,
    searchSessionId: String
  ): Future[Seq[EGaitKineticData]] =
    for {
      rawData <- downloadRawData(sessionToken, userSessionId, searchSessionId)
    } yield {
      val files = ZipFileIterator.asBytes(rawData.toSeq.toArray).toSeq

      val rightSensorKineticData = extractKineticData(files(1)._2).toSeq
      val leftSensorKineticData = extractKineticData(files(2)._2).toSeq

      val sessionXmlString = new String(files(0)._2, "UTF-8")
      val sessionXML = XML.loadString(sessionXmlString)

      val testInfos = parseTestInfos(sessionXML)

      testInfos.map { testInfo =>
        val testRightKineticData = rightSensorKineticData.slice(testInfo.rightStart, testInfo.rightStop)
        val testLeftKineticData = leftSensorKineticData.slice(testInfo.leftStart, testInfo.leftStop)

//        s"${testInfo.name}, ${testInfo.duration}:\nRight: ${testInfo.rightStart}, ${testInfo.rightStop}, size: ${testRightKineticData.size}\nLeft: ${testInfo.leftStart}, ${testInfo.leftStop}, size: ${testLeftKineticData.size}"

        EGaitKineticData(
          sessionId = (sessionXML \ "SessionId").text,
          personId = (sessionXML \ "PersonId").text,
          instructor = (sessionXML \ "Instructor").text,
          startTime = ConversionUtil.toDate(Seq(dateFormat))((sessionXML \ "Start").text),
          testName = testInfo.name,
          testDuration = testInfo.duration,
          rightSensorFileName = files(1)._1,
          leftSensorFileName = files(2)._1,
          rightSensorStartIndex = testInfo.rightStart,
          rightSensorStopIndex = testInfo.rightStop,
          leftSensorStartIndex = testInfo.leftStart,
          leftSensorStopIndex = testInfo.leftStop,
          rightAccelerometerPoints = testRightKineticData.map(_._1),
          rightGyroscopePoints = testRightKineticData.map(_._2),
          leftAccelerometerPoints = testLeftKineticData.map(_._1),
          leftGyroscopePoints = testLeftKineticData.map(_._2)
        )
      }
    }

  // the first component is accelerometer, the second one gyroscope
  private def extractKineticData(bytes: Array[Byte]): Iterator[(SpatialPoint, SpatialPoint)] =
    bytes.grouped(12).map { timePointData =>
      // little-endian conversion
//      def intValue(startIndex: Int) = ((timePointData(startIndex + 1) & 0xff) << 8) | (timePointData(startIndex) & 0xff)
      def intValue(startIndex: Int) = (timePointData(startIndex + 1) << 8 | timePointData(startIndex) & 0xff)

      val accelerometerPoint = SpatialPoint(x = intValue(0), y = intValue(2), z = intValue(4))
      val gyroscopePoint = SpatialPoint(x = intValue(6), y = intValue(8), z = intValue(10))
      (accelerometerPoint, gyroscopePoint)
    }

  private def parseTestInfos(sessionXML: Node): Seq[TestInfo] =
    (sessionXML \ "TestList" \ "Test").map { testXML =>
      val testName = (testXML \ "Name").text
      val duration = (testXML \ "Duration").text.toInt

      def posStartStop(moteXML: Node) = {
        val position  = (moteXML \ "Position").text
        val start = (moteXML \ "Tag" \ "Start").text.toInt
        val stop = (moteXML \ "Tag" \ "Stop").text.toInt
        (position, start, stop)
      }

      val moteXMLs = (testXML \ "MoteList" \ "Mote")
      if (moteXMLs.size != 2)
        throw new IllegalArgumentException("eGait Test XML " + testXML.toString + " do not contain two motes for and left and right sensors).")

      val startStops = moteXMLs.map(posStartStop)

      val rightStartStop = startStops.find(_._1.equals("RightFoot"))
      val leftStartStop = startStops.find(_._1.equals("LeftFoot"))

      if (rightStartStop.isEmpty && leftStartStop.isEmpty)
        throw new IllegalArgumentException("eGait Test XML " + testXML.toString + " do not contain two motes for and left and right sensors).")

      TestInfo(testName, duration, rightStartStop.get._2, rightStartStop.get._3, leftStartStop.get._2, leftStartStop.get._3)
    }

  private def withXmlContent(request: WSRequest): WSRequest =
    request.withHeaders("Content-Type" -> "application/xml")

  private def withConnectionToken(connectionToken: String)(request: WSRequest): WSRequest =
    request.withHeaders("connect-token" -> connectionToken)

  private def withUserSessionId(userSessionId: String)(request: WSRequest): WSRequest =
    request.withHeaders("user-session" -> userSessionId)

  private def withRequestTimeout(timeout: Duration)(request: WSRequest): WSRequest =
    request.withRequestTimeout(timeout)

  private def loadFileAsBase64(fileName : String):String = {
    val source = scala.io.Source.fromFile(fileName, "ISO-8859-1")
    val byteArray = source.map(_.toByte).toArray
    source.close()
    val encoded = Base64.encodeBase64(byteArray)
    new String(encoded, "ASCII")
  }
}