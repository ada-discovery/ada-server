package org.ada.server.services.transformers

import akka.stream.scaladsl.Source
import org.ada.server.calc.CalculatorHelper.RunExt
import org.ada.server.calc.impl.MultiAdapterCalc
import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.field.inference.{FieldTypeInferrer, FieldTypeInferrerFactory, FieldTypeInferrerTypePack}
import org.ada.server.models.datatrans.InferDataSetTransformation
import org.incal.core.dataaccess.NotEqualsNullCriterion
import org.incal.core.util.seqFutures
import play.api.libs.json.{JsObject, JsReadable, JsValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class InferDataSetTransformer extends AbstractDataSetTransformer[InferDataSetTransformation] {

  private val saveViewsAndFilters = true

  override protected def execInternal(
    spec: InferDataSetTransformation
  ) = {
    val sourceDsa = dsaSafe(spec.sourceDataSetId)

    val fieldTypeInferrerFactory = new FieldTypeInferrerFactory(
      FieldTypeHelper.fieldTypeFactory(booleanIncludeNumbers = spec.booleanIncludeNumbers),
      spec.maxEnumValuesCount.getOrElse(FieldTypeHelper.maxEnumValuesCount),
      spec.minAvgValuesPerEnum.getOrElse(FieldTypeHelper.minAvgValuesPerEnum),
      FieldTypeHelper.arrayDelimiter
    )

    val jfti = fieldTypeInferrerFactory.ofJson

    val inferenceGroupsInParallelInit = spec.inferenceGroupsInParallel.getOrElse(1)
    val inferenceGroupSizeInit = spec.inferenceGroupSize.getOrElse(1)


    for {
      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // infer new field types
      newFieldNameAndTypes <- {
        logger.info("Inferring new field types started")
        val fieldNames = fields.map(_.name).toSeq.sorted

        seqFutures(fieldNames.grouped(inferenceGroupsInParallelInit * inferenceGroupSizeInit)) { fieldsNames =>
          logger.info(s"Inferring new field types for ${fieldsNames.size} fields as a stream")
          inferFieldTypesInParallelAsStream(sourceDsa.dataSetRepo, fieldsNames, inferenceGroupSizeInit, jfti)
        }.map(_.flatten)
      }

      // construct new fields
      newFields = {
        val originalFieldNameMap = fields.map(field => (field.name, field)).toMap

        newFieldNameAndTypes.flatMap { case (fieldName, fieldType) =>
          val fieldTypeSpec = fieldType.spec
          val stringEnums = fieldTypeSpec.enumValues.map { case (from, to) => (from.toString, to)}

          originalFieldNameMap.get(fieldName).map( field =>
            field.copy(fieldType = fieldTypeSpec.fieldType, isArray = fieldTypeSpec.isArray, enumValues = stringEnums)
          )
        }
      }

      // input data stream
      inputStream <- sourceDsa.dataSetRepo.findAsStream()

      // transform the stream by applying inferred types and converting jsons
      newFieldNameAndTypeMap = newFieldNameAndTypes.toMap
      transformedStream = inputStream.map { json =>
        val newJsonValues = json.fields.map { case (fieldName, jsonValue) =>
          val newJsonValue = newFieldNameAndTypeMap.get(fieldName) match {
            case Some(newFieldType) => displayJsonToJson(newFieldType, jsonValue)
            case None => jsonValue
          }
          (fieldName, newJsonValue)
        }
        JsObject(newJsonValues)
      }

    } yield
      (sourceDsa, newFields, transformedStream, saveViewsAndFilters)
  }

  private def displayJsonToJson[T](
    fieldType: FieldType[T],
    json: JsReadable
  ): JsValue = {
    val value = fieldType.displayJsonToValue(json)
    fieldType.valueToJson(value)
  }

  private def inferFieldTypesInParallelAsStream(
    dataRepo: JsonCrudRepo,
    fieldNames: Traversable[String],
    groupSize: Int,
    jfti: FieldTypeInferrer[JsReadable]
  ): Future[Traversable[(String, FieldType[_])]] = {
    val groupedFieldNames = fieldNames.toSeq.grouped(groupSize).toSeq

    for {
      fieldNameAndTypes <- Future.sequence(
        groupedFieldNames.map { groupFieldNames =>
          // if we infer only one field we add a not-null criterion
          val criteria = if (groupFieldNames.size == 1) Seq(NotEqualsNullCriterion(groupFieldNames.head)) else Nil

          for {
            source <- dataRepo.findAsStream(criteria = criteria, projection = groupFieldNames)
            fieldTypes <- inferFieldTypesAsStream(jfti, groupFieldNames)(source)
          } yield
            fieldTypes
        }
      )
    } yield
      fieldNameAndTypes.flatten
  }

  private def inferFieldTypesAsStream(
    jfti: FieldTypeInferrer[JsReadable],
    fieldNames: Traversable[String])(
    source: Source[JsObject, _]
  ): Future[Traversable[(String, FieldType[_])]] = {
    val multiInferrer = MultiAdapterCalc(jfti, Some(fieldNames.size))
    val seqFieldNames = fieldNames.toSeq

    val individualFieldJsonSource: Source[Seq[JsReadable], _] =
      source.map(json =>
        seqFieldNames.map(fieldName => (json \ fieldName))
      )

    for {
      fieldTypes <- multiInferrer.runFlow((), ())(individualFieldJsonSource)
    } yield
      seqFieldNames.zip(fieldTypes)
  }
}