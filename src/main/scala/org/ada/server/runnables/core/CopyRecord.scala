package org.ada.server.runnables.core

import org.ada.server.AdaException
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import reactivemongo.bson.BSONObjectID
import runnables.DsaInputFutureRunnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf

class CopyRecord extends DsaInputFutureRunnable[RecordSpec] {

  private val idName = JsObjectIdentity.name

  override def runAsFuture(spec: RecordSpec) = {
    val repo = createDataSetRepo(spec.dataSetId)

    for {
      // get a requested record
      recordOption <- repo.get(spec.recordId)

      // clean id and save a copy
      _ <- recordOption.map( record => repo.save(record.-(idName))).getOrElse(
        throw new AdaException(s"Record ${spec.recordId} not found.")
      )
    } yield
      ()
  }
}

case class RecordSpec(dataSetId: String, recordId: BSONObjectID)