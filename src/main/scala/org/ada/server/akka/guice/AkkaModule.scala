package org.ada.server.akka.guice

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import com.google.inject.{AbstractModule, Injector, Provider}
import com.typesafe.config.Config
import net.codingwell.scalaguice.ScalaModule
import javax.inject.Inject
import org.ada.server.akka.guice.AkkaModule.{ActorSystemProvider, MaterializerProvider}

// TODO: Do we need it?
@Deprecated
object AkkaModule {
  private val name = "main-actor-system"

  class ActorSystemProvider @Inject() (val config: Config, val injector: Injector) extends Provider[ActorSystem] {
    override def get() = {
      val system = ActorSystem(name, config)
      GuiceAkkaExtension(system).initialize(injector)
      system
    }
  }

  class MaterializerProvider @Inject()(system: ActorSystem) extends Provider[Materializer] {

    override def get: Materializer = {
      val settings = ActorMaterializerSettings.create(system)
      ActorMaterializer.create(settings, system, name)
    }
  }
}

/**
 * A module providing an Akka ActorSystem.
 */
class AkkaModule extends AbstractModule with ScalaModule {

  override def configure() {
    bind[ActorSystem].toProvider[ActorSystemProvider].asEagerSingleton()
    bind[Materializer].toProvider[MaterializerProvider].asEagerSingleton()
  }
}