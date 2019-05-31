package org.ada.server.services.transformers

import java.util.Date

import javax.inject.Inject
import org.ada.server.models.datatrans.DataSetTransformation._
import org.ada.server.models.datatrans.DataSetTransformation
import org.ada.server.dataaccess.RepoTypes.{DataSetTransformationRepo, MessageRepo}
import org.ada.server.services.LookupCentralExecImpl
import org.ada.server.util.MessageLogger
import play.api.Logger
import play.api.inject.Injector

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

protected[services] class DataSetCentralTransformerImpl @Inject()(
  val injector: Injector,
  repo: DataSetTransformationRepo,
  messageRepo: MessageRepo
) extends LookupCentralExecImpl[DataSetTransformation, DataSetTransformer[DataSetTransformation]](
  "org.ada.server.services.transformers",
  "data set transformer"
) {

  private val logger = Logger
  private val messageLogger = MessageLogger(logger, messageRepo)

  override protected def postExec(
    input: DataSetTransformation,
    exec: DataSetTransformer[DataSetTransformation]
  ): Future[Unit] = {
    val updatedInput = input.copyWithTimestamps(timeCreated = input.timeCreated, timeLastExecuted = Some(new Date()))
    repo.update(updatedInput).map(_ =>
      messageLogger.info(s"Transformation of data set(s) '${input.sourceDataSetIds.mkString(", ")}' successfully finished.")
    )
  }
}