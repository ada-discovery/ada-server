package org.ada.server.services.transformers

import akka.actor.ActorSystem
import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.DataSetTransformationRepo
import org.ada.server.models.datatrans.DataSetTransformation.DataSetMetaTransformationIdentity
import org.ada.server.models.datatrans.DataSetMetaTransformation
import org.ada.server.services.{LookupCentralExec, SchedulerImpl}
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext

protected[services] class DataSetTransformationSchedulerImpl @Inject() (
  val system: ActorSystem,
  val repo: DataSetTransformationRepo,
  val execCentral: LookupCentralExec[DataSetMetaTransformation])(
  implicit ec: ExecutionContext
) extends SchedulerImpl[DataSetMetaTransformation, BSONObjectID]("data set transformation")