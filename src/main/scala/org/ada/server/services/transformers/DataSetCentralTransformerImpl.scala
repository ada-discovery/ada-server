package org.ada.server.services.transformers

import java.util.Date

import javax.inject.Inject
import org.ada.server.models.datatrans.DataSetTransformation._
import org.ada.server.models.datatrans.DataSetMetaTransformation
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
) extends LookupCentralExecImpl[DataSetMetaTransformation, DataSetMetaTransformer[DataSetMetaTransformation]](
  "org.ada.server.services.transformers",
  "data set transformer"
) {

  private val logger = Logger
  private val messageLogger = MessageLogger(logger, messageRepo)

  override protected def postExec(
    input: DataSetMetaTransformation,
    exec: DataSetMetaTransformer[DataSetMetaTransformation]
  ) =
    for {
      _ <- if (input._id.isDefined) {
        // update if id exists, i.e., it's a persisted transformation
        val updatedInput = input.copyCore(input._id, input.timeCreated, Some(new Date()), input.scheduled, input.scheduledTime)
        repo.update(updatedInput).map(_ => ())
      } else
        Future(())

    } yield
      messageLogger.info(s"Transformation of data set(s) '${input.sourceDataSetIds.mkString(", ")}' successfully finished.")
}