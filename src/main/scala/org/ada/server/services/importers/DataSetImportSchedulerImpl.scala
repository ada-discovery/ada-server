package org.ada.server.services.importers

import akka.actor.ActorSystem
import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.DataSetImportRepo
import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.models.dataimport.DataSetImport.DataSetImportIdentity
import org.ada.server.services.{LookupCentralExec, SchedulerImpl}
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext

protected[services] class DataSetImportSchedulerImpl @Inject() (
  val system: ActorSystem,
  val repo: DataSetImportRepo,
  val execCentral: LookupCentralExec[DataSetImport])(
  implicit ec: ExecutionContext
) extends SchedulerImpl[DataSetImport, BSONObjectID]("data set import")