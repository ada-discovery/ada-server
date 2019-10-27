package org.ada.server.services.importers

import java.util.Date

import javax.inject.Inject
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
  ) =
    for {
      _ <- if (input._id.isDefined) {
        // update if id exists, i.e., it's a persisted import
        val updatedInput = input.copyCore(input._id, input.timeCreated, Some(new Date()), input.scheduled, input.scheduledTime)
        repo.update(updatedInput).map(_ => ())
      } else
        Future(())

    } yield
      messageLogger.info(s"Import of data set '${input.dataSetName}' successfully finished.")
}