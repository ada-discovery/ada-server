package services

import javax.inject.Singleton
import net.codingwell.scalaguice.ScalaModule
import play.api.libs.ws.{WSAPI, WSClient, WSClientConfig, WSConfigParser}
import play.api.libs.ws.ahc.{AhcWSAPI, AhcWSClientConfig, AhcWSClientConfigParser, WSClientProvider}

class AhcWSModule extends ScalaModule {
  override def configure() {
    bind[WSAPI].to[AhcWSAPI]
    bind[AhcWSClientConfig].toProvider[AhcWSClientConfigParser].in[Singleton]
    bind[WSClientConfig].toProvider[WSConfigParser].in[Singleton]
    bind[WSClient].toProvider[WSClientProvider].in[Singleton]
  }
}