package org.ada.server.services.transformers

import akka.actor.ActorSystem
import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.DataSetTransformationRepo
import org.ada.server.models.datatrans.DataSetTransformation.DataSetTransformationIdentity
import org.ada.server.models.datatrans.DataSetTransformation
import org.ada.server.services.{LookupCentralExec, SchedulerImpl}
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext

protected[services] class DataSetTransformationSchedulerImpl @Inject() (
  val system: ActorSystem,
  val repo: DataSetTransformationRepo,
  val execCentral: LookupCentralExec[DataSetTransformation])(
  implicit ec: ExecutionContext
) extends SchedulerImpl[DataSetTransformation, BSONObjectID]("data set transformation")