package services

import org.ada.server.services.importers.{RedCapLockAction, RedCapServiceFactory}
import org.ada.server.services.GuiceApp
import net.codingwell.scalaguice.InjectorExtensions._
import org.ada.server.models.redcap.JsonFormat.responseFormat
import org.scalatest._
import play.api.Configuration
import play.api.libs.json.Json

@Deprecated
class RedCapServiceTest extends AsyncFlatSpec with Matchers {

  private val injector = GuiceApp(Seq(
    new org.ada.server.services.ConfigModule(),
    new org.ada.server.akka.guice.AkkaModule(),
    new org.ada.server.services.WebServiceModule(),
    new services.AhcWSModule()
  ))

  private val redCapServiceFactory = injector.instance[RedCapServiceFactory]
  private val configuration = injector.instance[Configuration]

//  private val redCapService = redCapServiceFactory(
//    configuration.getString("test.redcap.url").get,
//    configuration.getString("test.redcap.token").get
//  )

//  "Locking" should "lock the records" in {
//    redCapService.lock(RedCapLockAction.lock, "ND00001").map { results =>
//      println("Response:")
//      println(results.map(x => Json.prettyPrint(Json.toJson(x))).mkString("\n"))
//      results.size should be (1)
//    }
//  }
}