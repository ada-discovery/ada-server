package org.ada.server.services.transformers

import org.ada.server.AdaException
import org.ada.server.dataaccess.dataset.DataSetAccessor
import org.ada.server.field.FieldUtil.{FieldOps, NamedFieldType}
import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.ada.server.models.Field
import org.ada.server.models.datatrans.{DataSetTransformation, LinkedDataSetSpec}
import org.incal.core.dataaccess.Criterion._
import play.api.libs.json.JsObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait LinkDataSetsHelper[T <: DataSetTransformation] {

  this: AbstractDataSetTransformer[T] =>

  protected def createDataSetInfo(
    spec: LinkedDataSetSpec
  ): Future[LinkedDataSetInfo] = {
    // data set accessor
    val dsa = dsaSafe(spec.dataSetId)

    // determine which fields to load (depending on whether preserve field names are specified)
    val fieldNamesToLoad = spec.explicitFieldNamesToKeep match {
      case Nil => Nil
      case _ => (spec.explicitFieldNamesToKeep ++ spec.linkFieldNames).toSet
    }

    for {
      // load fields
      fields <- fieldNamesToLoad match {
        case Nil => dsa.fieldRepo.find()
        case _ => dsa.fieldRepo.find(Seq(FieldIdentity.name #-> fieldNamesToLoad.toSeq))
      }
    } yield {
      // collect field types (in order) for the link
      val nameFieldMap = fields.map(field => (field.name, field)).toMap

      val linkFieldTypes = spec.linkFieldNames.map { fieldName =>
        nameFieldMap.get(fieldName).map(_.toNamedTypeAny).getOrElse(
          throw new AdaException(s"Link field $fieldName not found.")
        )
      }

      LinkedDataSetInfo(dsa, spec.linkFieldNames, fields, linkFieldTypes)
    }
  }

  protected def stripJson(
    linkFieldNameSet: Set[String],
    dataSetIdPrefixToAdd: Option[String])(
    json: JsObject
  ) = {
    // remove the link fields from a json
    val strippedJson = json.fields.filterNot { case (fieldName, _) => linkFieldNameSet.contains(fieldName) }

    // rename if necessary
    val renamedJson = if (dataSetIdPrefixToAdd.isDefined) {
      val prefix = dataSetIfFieldPrefix(dataSetIdPrefixToAdd.get)
      strippedJson.map { case (fieldName, jsValue) => (prefix + fieldName, jsValue) }
    } else
      strippedJson

    JsObject(renamedJson)
  }

  // collect all the fields for the new data setor
  protected def createResultFields(
    leftDataSetInfo: LinkedDataSetInfo,
    rightDataSetInfos: Seq[LinkedDataSetInfo],
    addDataSetIdToRightFieldNames: Boolean
  ) = {
    val rightFieldsWoLink = rightDataSetInfos.flatMap { rightDataSetInfo =>

      val linkFieldNameSet = rightDataSetInfo.linkFieldNames.toSet
      val fieldsWoLink = rightDataSetInfo.fields.filterNot {field => linkFieldNameSet.contains(field.name)}.toSeq

      if (addDataSetIdToRightFieldNames)
        fieldsWoLink.map { field =>
          val newFieldName = dataSetIfFieldPrefix(rightDataSetInfo.dsa.dataSetId) + field.name
          field.copy(name = newFieldName)
        }
      else
        fieldsWoLink
    }

    leftDataSetInfo.fields ++ rightFieldsWoLink
  }

  private def dataSetIfFieldPrefix(dataSetId: String) =
    dataSetId.replace('.', '_') + "-"
}

case class LinkedDataSetInfo(
  dsa: DataSetAccessor,
  linkFieldNames: Seq[String],
  fields: Traversable[Field],
  linkFieldTypes: Seq[NamedFieldType[Any]]
) {
  val fieldNames = fields.map(_.name)
}