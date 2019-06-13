package org.ada.server.services

import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.models.datatrans.DataSetMetaTransformation
import reactivemongo.bson.BSONObjectID

object ServiceTypes {
  type DataSetCentralImporter = LookupCentralExec[DataSetImport]
  type DataSetImportScheduler = Scheduler[DataSetImport, BSONObjectID]

  type DataSetCentralTransformer = LookupCentralExec[DataSetMetaTransformation]
  type DataSetTransformationScheduler = Scheduler[DataSetMetaTransformation, BSONObjectID]
}
