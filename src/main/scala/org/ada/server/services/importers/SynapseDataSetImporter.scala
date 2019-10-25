package org.ada.server.services.importers

import java.util.Date

import javax.inject.Inject
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import org.ada.server.models.dataimport.SynapseDataSetImport
import org.ada.server.AdaException
import org.ada.server.models._
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.synapse._
import org.ada.server.services.importers.SynapseServiceFactory
import org.ada.server.dataaccess.JsonUtil
import org.ada.server.field.FieldUtil.specToField
import org.incal.core.util.nonAlphanumericToUnderscore
import play.api.Configuration
import play.api.libs.json.{JsArray, JsObject, Json}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class SynapseDataSetImporter @Inject() (
    synapseServiceFactory: SynapseServiceFactory,
    configuration: Configuration
  ) extends AbstractDataSetImporter[SynapseDataSetImport] {

  private val synapseDelimiter = ','
  private val synapseEol = "\n"
  private val synapseBulkDownloadAttemptNumber = 4
  private val synapseDefaultBulkDownloadGroupNumber = 5
  private val keyFieldName = "ROW_ID"
  private val maxEnumValuesCount = 150

  private val synapseFtf = FieldTypeHelper.fieldTypeFactory(FieldTypeHelper.nullAliases ++ Set("nan"))
  private val fti = FieldTypeHelper.fieldTypeInferrerFactory(synapseFtf, maxEnumValuesCount).ofString

  private val prefixSuffixSeparators = Seq(
    ("\"[\"\"", "\"\"]\""),
    ("\"[", "]\""),
    ("\"", "\"")
  )

  private lazy val synapseUsername = confValue("synapse.api.username")
  private lazy val synapsePassword = confValue("synapse.api.password")

  private def confValue(key: String) = configuration.getString(key).getOrElse(
    throw new AdaException(s"Configuration entry '$key' not specified.")
  )

  override def runAsFuture(importInfo: SynapseDataSetImport): Future[Unit] = {
    try {
      val synapseService = synapseServiceFactory(synapseUsername, synapsePassword)

      def fieldName(column: ColumnModel) =
        nonAlphanumericToUnderscore(column.name.replaceAll("\"", "").trim)

      if (importInfo.downloadColumnFiles)
        for {
          // get the columns of the "file" and "entity" type (max 1)
          (entityColumnName, fileFieldNames) <- synapseService.getTableColumnModels(importInfo.tableId).map { columnModels =>
            val fileColumns = columnModels.results.filter(_.columnType == ColumnType.FILEHANDLEID).map(fieldName)
            val entityColumn = columnModels.results.find(_.columnType == ColumnType.ENTITYID).map(fieldName)
            (entityColumn, fileColumns)
          }

          _ <- {
            val bulkDownloadGroupNumber = importInfo.bulkDownloadGroupNumber.getOrElse(synapseDefaultBulkDownloadGroupNumber)

            val fun = updateJsonsFileFields(
              synapseService,
              fileFieldNames,
              entityColumnName,
              importInfo.tableId,
              bulkDownloadGroupNumber,
              Some(synapseBulkDownloadAttemptNumber)
            ) _

            importDataSetAux(importInfo, synapseService, fileFieldNames, Some(fun), importInfo.batchSize)
          }
        } yield
          ()
      else
        importDataSetAux(importInfo, synapseService, Nil, None, importInfo.batchSize).map(_ => ())
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  private def importDataSetAux(
    importInfo: SynapseDataSetImport,
    synapseService: SynapseService,
    fileFieldNames: Traversable[String],
    transformJsonsFun: Option[Seq[JsObject] => Future[(Seq[JsObject])]],
    batchSize: Option[Int]
  ) = {
    logger.info(new Date().toString)
    logger.info(s"Import of data set '${importInfo.dataSetName}' initiated.")

    val delimiter = synapseDelimiter.toString

    try {
      for {
        // create/retrieve a dsa
        dsa <- createDataSetAccessor(importInfo)

        csv <- {
          logger.info("Downloading CSV table from Synapse...")
          synapseService.getTableAsCsv(importInfo.tableId)
        }

        // get all the fields
        fields <- dsa.fieldRepo.find()

        // create jsons and field types
        (jsons: Seq[JsObject], newFields) = {
          logger.info(s"Parsing lines and inferring types...")
          extractJsonsAndInferFieldsFromCSV(csv, delimiter, fields, fileFieldNames)
        }

        // save, or update the dictionary
        _ <- dataSetService.updateFields(importInfo.dataSetId, newFields, true, true)

        // since we possible changed the dictionary (the data structure) we need to update the data set repo
        _ <- dsa.updateDataSetRepo

        // get the new data set repo
        dataRepo = dsa.dataSetRepo

        // transform jsons (if needed) and save (and update) the jsons
        _ <- {
          if (transformJsonsFun.isDefined)
            logger.info(s"Saving and transforming JSONs...")
          else
            logger.info(s"Saving JSONs...")

          dataSetService.saveOrUpdateRecords(dataRepo, jsons, Some(keyFieldName), false, transformJsonsFun, batchSize)
        }

        // remove the old records
        _ <- {
          val keyField = newFields.find(_.name == keyFieldName).getOrElse(
            throw new AdaException(s"Synapse key field $keyFieldName not found.")
          )
          val keyFieldType = synapseFtf(keyField.fieldTypeSpec)
          val keys = JsonUtil.project(jsons, keyFieldName).map(keyFieldType.jsonToValue)
          dataSetService.deleteRecordsExcept(dataRepo, keyFieldName, keys.flatten.toSeq)
        }
      } yield
        ()
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  private def extractJsonsAndInferFieldsFromCSV(
    csv: String,
    delimiter: String,
    fields: Traversable[Field],
    fileFieldNames: Traversable[String]
  ): (Seq[JsObject], Seq[Field]) = {
    // split by lines
    val lines = csv.split(synapseEol).iterator

    // collect the column names and labels
    val columnNamesAndLabels = dataSetService.getColumnNameLabels(delimiter, lines)

    // parse lines
    val values = dataSetService.parseLines(columnNamesAndLabels.size, lines, delimiter, true, prefixSuffixSeparators)

    // create jsons and field types
    createSynapseJsonsWithFields(columnNamesAndLabels, fileFieldNames.toSet, values.toSeq, fields)
  }

  protected def updateJsonsFileFields(
    synapseService: SynapseService,
    fileFieldNames: Seq[String],
    entityColumnName: Option[String],
    tableId: String,
    bulkDownloadGroupNumber: Int,
    bulkDownloadAttemptNumber: Option[Int])(
    jsons: Seq[JsObject]
  ): Future[Seq[JsObject]] = {
    val fileHandleEntityIds = fileFieldNames.flatMap( fieldName =>
      jsons.flatMap { json =>
        JsonUtil.toString(json \ fieldName).map { fileHandleId =>
          val entityId = entityColumnName.flatMap(name => JsonUtil.toString(json \ name))
          (fileHandleId, entityId)
        }
      }
    )

    if (fileHandleEntityIds.nonEmpty) {
      val groupSize = Math.max(fileHandleEntityIds.size / bulkDownloadGroupNumber, 1)
      val groups = {
        if (fileHandleEntityIds.size.toDouble / groupSize > bulkDownloadGroupNumber)
          fileHandleEntityIds.grouped(groupSize + 1)
        else
          fileHandleEntityIds.grouped(groupSize)
      }.toSeq

      logger.info(s"Bulk download of Synapse column data for ${fileHandleEntityIds.size} file handles (split into ${groups.size} groups) initiated.")

      // download the files in a bulk
      val fileHandleIdContentsFutures = groups.par.map { groupFileHandleEntityIds =>
        val assocs = groupFileHandleEntityIds.map { case (handleId, entityId) =>
          entityId match {
            case None => FileHandleAssociation(FileHandleAssociateType.TableEntity, handleId, tableId)
            case Some(entityId) =>  FileHandleAssociation(FileHandleAssociateType.FileEntity, handleId, entityId)
          }

        }
        synapseService.downloadFilesInBulk(assocs, bulkDownloadAttemptNumber)
      }

      Future.sequence(fileHandleIdContentsFutures.toList).map(_.flatten).map { fileHandleIdContents =>
        logger.info(s"Download of ${fileHandleIdContents.size} Synapse column data finished. Updating JSONs with Synapse column data...")
        val fileHandleIdContentMap = fileHandleIdContents.toMap
        // update jsons with new file contents
        val newJsons = jsons.map { json =>
          val fieldNameJsons = fileFieldNames.flatMap( fieldName =>
            JsonUtil.toString(json \ fieldName).map { fileHandleId =>
              val fileContent = fileHandleIdContentMap.get(fileHandleId).get

              def extractJsonsFromCSV = {
                val (jsons, _) = extractJsonsAndInferFieldsFromCSV(fileContent, "\t", Nil, Nil)
                JsArray(jsons)
              }

              val columnJson =
                if (entityColumnName.isDefined) {
                  extractJsonsFromCSV
                } else {
                  try {
                    Json.parse(fileContent)
                  } catch {
                    // if it cannot be parsed as JSON try to treat it as a CSV file
                    case e: JsonParseException => extractJsonsFromCSV
                    case e: JsonMappingException => extractJsonsFromCSV
                  }
                }

              (fieldName, columnJson)
            }
          )
          json ++ JsObject(fieldNameJsons)
        }

        newJsons
      }
    } else
    // no update
      Future(jsons)
  }

  private def createSynapseJsonsWithFields(
    fieldNamesAndLabels: Seq[(String, String)],
    fileFieldNames: Set[String],
    values: Seq[Seq[String]],
    fields: Traversable[Field]
  ): (Seq[JsObject], Seq[Field]) = {

    // get the existing types
    val existingFieldTypeMap: Map[String, FieldType[_]] = fields.map ( field => (field.name, ftf(field.fieldTypeSpec))).toMap

    // infer the new types
    val newFieldTypes = values.transpose.par.map(fti.apply).toList

    // merge the old and new field types
    val fieldNameAndTypesForParsing = fieldNamesAndLabels.zip(newFieldTypes).map { case ((fieldName, _), newFieldType) =>
      val fieldType =
        if (fileFieldNames.contains(fieldName))
          newFieldType
        else
          existingFieldTypeMap.get(fieldName) match {
            case Some(existingFieldType) => mergeEnumTypes(existingFieldType, newFieldType)
            case None => newFieldType
          }
      (fieldName, fieldType)
    }

    // parse strings and create jsons
    val jsons = values.map( vals =>
      JsObject(
        (fieldNameAndTypesForParsing, vals).zipped.map {
          case ((fieldName, fieldType), text) =>
            val jsonValue = fieldType.displayStringToJson(text)
            (fieldName, jsonValue)
        })
    )

    // create the final field types... for file fields report JSON array if the type does not exist
    val finalFields = fieldNamesAndLabels.zip(newFieldTypes).map { case ((fieldName, label), newFieldType) =>

      val fieldType: FieldType[_] = existingFieldTypeMap.get(fieldName) match {
        case Some(existingFieldType) => mergeEnumTypes(existingFieldType, newFieldType)
        case None => if (fileFieldNames.contains(fieldName))
          ftf(FieldTypeId.Json, true)
        else
          newFieldType
      }

      specToField(fieldName, Some(label), fieldType.spec)
    }

    (jsons, finalFields)
  }

  private def mergeEnumTypes(oldType: FieldType[_], newType: FieldType[_]): FieldType[_] = {
    val oldTypeSpec = oldType.spec
    val newTypeSpec = newType.spec

    if (oldTypeSpec.fieldType == FieldTypeId.Enum && newTypeSpec.fieldType == FieldTypeId.Enum) {
        val newValues = newTypeSpec.enumValues.map(_._2).toBuffer
        val oldValues = oldTypeSpec.enumValues.map(_._2)

        val extraValues = newValues.--(oldValues).sorted
        val maxKey = oldTypeSpec.enumValues.map(_._1).max
        val extraEnumMap = extraValues.zipWithIndex.map { case (value, index) => (maxKey + index + 1, value)}

        val mergedFieldTypeSpec = oldTypeSpec.copy(enumValues = oldTypeSpec.enumValues ++ extraEnumMap)
        ftf(mergedFieldTypeSpec)
    } else
      oldType
  }
}