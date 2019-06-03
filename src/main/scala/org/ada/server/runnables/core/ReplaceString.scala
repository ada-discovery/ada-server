package org.ada.server.runnables.core

import org.ada.server.dataaccess.RepoTypes.{FieldRepo, JsonCrudRepo}
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.models.{Field, FieldTypeId}
import org.ada.server.AdaException
import org.incal.core.util.seqFutures
import play.api.libs.json.{JsNull, JsString, JsValue, Json}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._
import runnables.DsaInputFutureRunnable
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps, NamedFieldType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe.typeOf

class ReplaceString extends DsaInputFutureRunnable[ReplaceStringSpec] {

  private val idName = JsObjectIdentity.name

  override def runAsFuture(spec: ReplaceStringSpec) = {
    val dsa = createDsa(spec.dataSetId)

    for {
      // field
      fieldOption <- dsa.fieldRepo.get(spec.fieldName)
      field = fieldOption.getOrElse(throw new AdaException(s"Field ${spec.fieldName} not found."))

      // replace for String or Enum
      _ <- field.fieldType match {
        case FieldTypeId.String => replaceForString(dsa.dataSetRepo, field.toNamedType[String], spec)
        case FieldTypeId.Enum => replaceForEnum(dsa.fieldRepo, field, spec)
        case _ => throw new AdaException(s"String replacement is possible only for String and Enum types but got ${field.fieldTypeSpec}.")
      }
    } yield
      ()
  }

  private def replaceForString(
    repo: JsonCrudRepo,
    fieldType: NamedFieldType[String],
    spec: ReplaceStringSpec
  ) = {
    for {
      // jsons
      idJsons <- repo.find(projection = Seq(idName, spec.fieldName))

      // get the records as String and replace
      idReplacedStringValues = idJsons.map { json =>
        val id = (json \ JsObjectIdentity.name).as[BSONObjectID]
        val replacedStringValue = json.toValue(fieldType).map(_.replaceAllLiterally(spec.from, spec.to))
        (id, replacedStringValue)
      }

      // replace the values and update the records
      _ <- seqFutures(idReplacedStringValues.toSeq.grouped(spec.batchSize)) { group =>
        for {
          newJsons <- Future.sequence(
            group.map { case (id, replacedStringValue) =>
              repo.get(id).map { json =>
                val jsValue: JsValue = replacedStringValue.map(JsString(_)).getOrElse(JsNull)
                json.get ++ Json.obj(spec.fieldName -> jsValue)
              }
            }
          )

          _ <- repo.update(newJsons)
        } yield
          ()
      }
    } yield
      ()
  }

  private def replaceForEnum(
    repo: FieldRepo,
    field: Field,
    spec: ReplaceStringSpec
  ) = {
    val newNumValue = field.enumValues.map { case (key, value) => (key, value.replaceAllLiterally(spec.from, spec.to))}
    repo.update(field.copy(enumValues = newNumValue))
  }
}

case class ReplaceStringSpec(dataSetId: String, fieldName: String, batchSize: Int, from: String, to: String)