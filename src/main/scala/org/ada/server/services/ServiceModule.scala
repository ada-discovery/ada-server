package org.ada.server.services

import com.google.inject.assistedinject.FactoryModuleBuilder
import org.ada.server.dataaccess.ignite.IgniteFactory
import net.codingwell.scalaguice.ScalaModule
import org.ada.server.services.importers._
import org.apache.ignite.Ignite

class ServiceModule extends ScalaModule {

  override def configure = {
    bind[Ignite].toProvider(classOf[IgniteFactory]).asEagerSingleton

    bind[DataSetImportScheduler].to(classOf[DataSetImportSchedulerImpl]).asEagerSingleton

    install(new FactoryModuleBuilder()
      .implement(classOf[SynapseService], classOf[SynapseServiceWSImpl])
      .build(classOf[SynapseServiceFactory]))

    install(new FactoryModuleBuilder()
      .implement(classOf[EGaitService], classOf[EGaitServiceWSImpl])
      .build(classOf[EGaitServiceFactory]))

    install(new FactoryModuleBuilder()
      .implement(classOf[RedCapService], classOf[RedCapServiceWSImpl])
      .build(classOf[RedCapServiceFactory]))
  }
}