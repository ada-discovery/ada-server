package org.ada.server.dataaccess.elastic

import javax.inject.Inject
import com.sksamuel.elastic4s.http.HttpClient
import org.incal.access.elastic.ElasticClientProvider
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

class PlayElasticClientProvider extends ElasticClientProvider {

  @Inject private var configuration: Configuration = _
  @Inject private var lifecycle: ApplicationLifecycle = _

  override protected def config = configuration.underlying

  override protected def shutdownHook(client: HttpClient): Unit =
    lifecycle.addStopHook(() => Future.successful(client.close()))
}