package org.ada.server.services

import com.google.inject.assistedinject.FactoryModuleBuilder
import net.codingwell.scalaguice.ScalaModule
import org.ada.server.services.ServiceTypes.{DataSetCentralImporter, DataSetCentralTransformer, DataSetImportScheduler, DataSetTransformationScheduler}
import org.ada.server.services.importers._
import org.ada.server.services.transformers.{DataSetCentralTransformerImpl, DataSetTransformationSchedulerImpl}

class ServiceModule extends WebServiceModule {

  override def configure = {

    super.configure

    bind[DataSetCentralImporter].to(classOf[DataSetCentralImporterImpl]).asEagerSingleton
    bind[DataSetImportScheduler].to(classOf[DataSetImportSchedulerImpl]).asEagerSingleton

    bind[DataSetCentralTransformer].to(classOf[DataSetCentralTransformerImpl]).asEagerSingleton
    bind[DataSetTransformationScheduler].to(classOf[DataSetTransformationSchedulerImpl]).asEagerSingleton
  }
}

class WebServiceModule extends ScalaModule {

  override def configure = {

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