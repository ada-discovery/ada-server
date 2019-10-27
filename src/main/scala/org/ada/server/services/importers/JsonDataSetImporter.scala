package org.ada.server.services.importers

import java.nio.charset.Charset
import java.util.Date

import org.ada.server.dataaccess._
import org.ada.server.field.{FieldType, FieldTypeFactory, FieldTypeHelper}
import org.ada.server.models.dataimport.JsonDataSetImport
import play.api.libs.json._
import org.ada.server.field.FieldUtil.specToField
import org.ada.server.field.inference.FieldTypeInferrerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

private class JsonDataSetImporter extends AbstractDataSetImporter[JsonDataSetImport] {

  override def runAsFuture(importInfo: JsonDataSetImport): Future[Unit] = {
    logger.info(new Date().toString)
    logger.info(s"Import of data set '${importInfo.dataSetName}' initiated.")

    try {
      val charset = Charset.forName(importInfo.charsetName.getOrElse(defaultCharset))
      def source = Source.fromFile(importInfo.path.get)(charset)

      // TODO: use an input stream here
      val fileContent = source.mkString

      val maxEnumValuesCount = importInfo.inferenceMaxEnumValuesCount.getOrElse(FieldTypeHelper.maxEnumValuesCount)
      val minAvgValuesPerEnum = importInfo.inferenceMinAvgValuesPerEnum.getOrElse(FieldTypeHelper.minAvgValuesPerEnum)

      val ftf = FieldTypeHelper.fieldTypeFactory(booleanIncludeNumbers = importInfo.booleanIncludeNumbers)
      val ftif = new FieldTypeInferrerFactory(ftf, maxEnumValuesCount, minAvgValuesPerEnum, FieldTypeHelper.arrayDelimiter)
      val jsonFti = ftif.ofJson

      Json.parse(fileContent) match {
        case JsArray(items) =>
          val jsons = items.map(_.as[JsObject])
          val fieldNames = jsons.flatMap { json => json.fields.map(_._1) }.toSet

          val fieldNameTypes = fieldNames.par.map { fieldName =>
            val jsValues = JsonUtil.project(jsons, fieldName)
            (fieldName, jsonFti(jsValues))
          }.toList

          val fieldNameTypeMap = fieldNameTypes.map { case (fieldName, fieldType) => (fieldName, fieldType.asValueOf[Any]) }.toMap

          // helper functions to parse jsons
          def displayJsonToJson[T](fieldType: FieldType[T], json: JsReadable): JsValue = {
            val value = fieldType.displayJsonToValue(json)
            fieldType.valueToJson(value)
          }

          // create new jsons
          val newJsons = jsons.map { originalJson =>
            val newJsonValues = originalJson.fields.map { case (fieldName, jsonValue) =>
              val newJsonValue = fieldNameTypeMap.get(fieldName) match {
                case Some(fieldType) => displayJsonToJson(fieldType, jsonValue)
                case None => jsonValue
              }
              (fieldName, newJsonValue)
            }
            JsObject(newJsonValues)
          }

          for {
            // create/retrieve a dsa
            dsa <- createDataSetAccessor(importInfo)

            // save the fields
            _ <- {
              val fields = fieldNameTypes.map { case (fieldName, fieldType) => specToField(fieldName, Some(fieldName), fieldType.spec) }
              dataSetService.updateFields(importInfo.dataSetId, fields, true, true)
            }

            // since we possible changed the dictionary (the data structure) we need to update the data set repo
            _ <- dsa.updateDataSetRepo

            // delete the old data
            _ <- dsa.dataSetRepo.deleteAll

            // save the data
            _ <- dataSetService.saveOrUpdateRecords(dsa.dataSetRepo, newJsons, None, false, None, importInfo.saveBatchSize)
          } yield
            ()

        case _ =>
          throw new AdaConversionException(s"File ${importInfo.path.get} is expected to contain a JSON array.")
      }
    } catch {
      case e: Exception => Future.failed(e)
    }
  }
}