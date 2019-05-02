package org.ada.server.services.importers

import java.util.Date

import com.google.inject.ImplementedBy
import org.ada.server.models._
import javax.inject.{Inject, Singleton}
import org.ada.server.models._
import org.ada.server.models.dataimport._
import org.ada.server.dataaccess.RepoTypes.DataSetImportRepo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@ImplementedBy(classOf[DataSetImporterCentralImpl])
trait DataSetImporterCentral {
  def apply(dataSetImport: DataSetImport): Future[Unit]
}

@Singleton
class DataSetImporterCentralImpl @Inject()(
    csvDataSetImporter: CsvDataSetImporter,
    tranSmartDataSetImporter: TranSmartDataSetImporter,
    redCapDataSetImporter: RedCapDataSetImporter,
    jsonDataSetImporter: JsonDataSetImporter,
    synapseDataSetImporter: SynapseDataSetImporter,
    eGaitDataSetImporter: EGaitDataSetImporter,
    dataSetImportRepo: DataSetImportRepo
  ) extends DataSetImporterCentral {

  override def apply(dataSetImport: DataSetImport): Future[Unit] = {
    for {
      _ <- dataSetImport match {
        case x: CsvDataSetImport => csvDataSetImporter(x)
        case x: JsonDataSetImport => jsonDataSetImporter(x)
        case x: TranSmartDataSetImport => tranSmartDataSetImporter(x)
        case x: RedCapDataSetImport => redCapDataSetImporter(x)
        case x: SynapseDataSetImport => synapseDataSetImporter(x)
        case x: EGaitDataSetImport => eGaitDataSetImporter(x)
      }

      _ <- {
        dataSetImport.timeLastExecuted = Some(new Date())
        dataSetImportRepo.update(dataSetImport)
      }
    } yield ()
  }
}