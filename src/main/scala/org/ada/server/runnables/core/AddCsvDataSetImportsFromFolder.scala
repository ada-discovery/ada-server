package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.models.{DataSetSetting, StorageType}
import org.ada.server.models.dataimport.CsvDataSetImport
import org.ada.server.dataaccess.RepoTypes.DataSetImportRepo
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.core.util.listFiles

import scala.concurrent.ExecutionContext.Implicits.global

class AddCsvDataSetImportsFromFolder @Inject()(
    dataSetImportRepo: DataSetImportRepo
  ) extends InputFutureRunnableExt[AddCsvDataSetImportsFromFolderSpec] {

  override def runAsFuture(spec: AddCsvDataSetImportsFromFolderSpec) = {
    val csvImports = listFiles(spec.folderPath).map { importFile =>
      val importFileName = importFile.getName
      val importFileNameWoExt = importFileName.replaceAll("\\.[^.]*$", "")

      val dataSetId = spec.dataSetIdPrefix + "." + importFileNameWoExt
      val dataSetName = spec.dataSetNamePrefix + " " + importFileNameWoExt

      val dataSetSetting = new DataSetSetting(dataSetId, spec.storageType)

      CsvDataSetImport(
        None,
        spec.dataSpaceName,
        dataSetId,
        dataSetName,
        Some(importFile.getAbsolutePath),
        spec.delimiter,
        None,
        None,
        true,
        true,
        booleanIncludeNumbers = false,
        saveBatchSize = spec.batchSize,
        setting = Some(dataSetSetting)
      )
    }

    dataSetImportRepo.save(csvImports).map(_ => ())
  }
}

case class AddCsvDataSetImportsFromFolderSpec(
  folderPath: String,
  dataSpaceName: String,
  dataSetIdPrefix: String,
  dataSetNamePrefix: String,
  delimiter: String,
  storageType: StorageType.Value,
  batchSize: Option[Int]
)