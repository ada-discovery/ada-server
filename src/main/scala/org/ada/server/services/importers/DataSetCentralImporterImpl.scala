package org.ada.server.services.importers

import java.util.Date

import javax.inject.Inject
import org.ada.server.models.dataimport.DataSetImport._
import org.ada.server.dataaccess.RepoTypes.{DataSetImportRepo, MessageRepo}
import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.services.LookupCentralExecImpl
import org.ada.server.util.MessageLogger
import play.api.Logger
import play.api.inject.Injector

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

protected[services] class DataSetCentralImporterImpl @Inject()(
  val injector: Injector,
  repo: DataSetImportRepo,
  messageRepo: MessageRepo
) extends LookupCentralExecImpl[DataSetImport, DataSetImporter[DataSetImport]](
  "org.ada.server.services.importers",
  "data set importer"
) {
  private val logger = Logger
  private val messageLogger = MessageLogger(logger, messageRepo)

  override protected def postExec(
    input: DataSetImport,
    exec: DataSetImporter[DataSetImport]
  ): Future[Unit] = {
    val updateInput = input.copyWithTimestamps(timeCreated = input.timeCreated, timeLastExecuted = Some(new Date()))
    repo.update(updateInput).map(_ =>
      messageLogger.info(s"Import of data set '${input.dataSetName}' successfully finished.")
    )
  }
}