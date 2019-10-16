package org.ada.server.services.importers

import net.codingwell.scalaguice.ScalaModule
import org.ada.server.Injector
import org.ada.server.models.dataimport.CsvDataSetImport
import org.ada.server.services.ServiceTypes.DataSetCentralImporter
import org.scalatest._

import scala.io.Codec

class CsvImporterSpec extends FlatSpec {
  private implicit val codec: Codec = Codec.UTF8
  private val irisCsv = getClass.getResource("/iris.csv").toString
  private val injector = new Injector(new ScalaModule {
    override def configure(): Unit =
      bind[DataSetCentralImporter].to(classOf[DataSetCentralImporterImpl])
  })

  "CsvDataSetImport" should "run without error" in {
    val importInfo = CsvDataSetImport(
      dataSpaceName = "",
      dataSetId = "iris",
      dataSetName = "iris",
      delimiter = ",",
      matchQuotes = false,
      inferFieldTypes = true,
      path = Some(irisCsv)
    )
    val importer = injector.getInstance[DataSetCentralImporter]
    importer(importInfo)
  }
}
