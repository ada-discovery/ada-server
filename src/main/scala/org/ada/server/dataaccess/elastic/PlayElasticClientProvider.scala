package org.ada.server.dataaccess.elastic

import javax.inject.Inject
import com.sksamuel.elastic4s.ElasticClient
import org.incal.access_elastic.ElasticClientProvider
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

class PlayElasticClientProvider extends ElasticClientProvider {

  @Inject private var configuration: Configuration = _
  @Inject private var lifecycle: ApplicationLifecycle = _

  override protected def config = configuration.underlying

  override protected def shutdownHook(client: ElasticClient): Unit =
    lifecycle.addStopHook(() => Future.successful(client.close()))
}