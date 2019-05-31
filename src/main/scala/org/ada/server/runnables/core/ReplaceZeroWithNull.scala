package org.ada.server.runnables.core

import java.{util => ju}

import org.incal.core.util.seqFutures
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.models.FieldTypeId
import org.ada.server.AdaException
import play.api.Logger
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._
import runnables.DsaInputFutureRunnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.runtime.universe.typeOf

class ReplaceZeroWithNull extends DsaInputFutureRunnable[ReplaceZeroWithNullSpec] {

  private val logger = Logger // (this.getClass())

  private val idName = JsObjectIdentity.name
  private val ftf = FieldTypeHelper.fieldTypeFactory()

  override def runAsFuture(spec: ReplaceZeroWithNullSpec) = {
    val dsa_ = createDsa(spec.dataSetId)
    val repo = dsa_.dataSetRepo

    for {
      // field
      fieldOption <- dsa_.fieldRepo.get(spec.fieldName)
      field = fieldOption.getOrElse(throw new AdaException(s"Field ${spec.fieldName} not found."))
      fieldType = ftf(field.fieldTypeSpec)

      // jsons
      idJsons <- repo.find(projection = Seq(idName, spec.fieldName))

      // get zero with null in the records
      idReplacedJsValues = idJsons.map { json =>
        val jsValue = (json \ spec.fieldName)

        val isZero =
          field.fieldTypeSpec.fieldType match {
            case FieldTypeId.Double =>
              fieldType.asValueOf[Double].jsonToValue(jsValue).map(_.equals(0d)).getOrElse(false)

            case FieldTypeId.Integer =>
              fieldType.asValueOf[Long].jsonToValue(jsValue).map(_.equals(0l)).getOrElse(false)

            case FieldTypeId.Date =>
              fieldType.asValueOf[ju.Date].jsonToValue(jsValue).map(_.getTime.equals(0l)).getOrElse(false)

            case _ => throw new AdaException(s"Zero-to-null conversion is possible only for Double, Integer, and Date types but got ${field.fieldTypeSpec}.")
          }

        val id = (json \ JsObjectIdentity.name).as[BSONObjectID]
        (id, if (isZero) JsNull else jsValue.getOrElse(JsNull))
      }

      // replace the values and update the records
      _ <- seqFutures(idReplacedJsValues.toSeq.grouped(spec.batchSize).zipWithIndex) { case (group, index) =>
        logger.info(s"Processing items ${spec.batchSize * index} to ${spec.batchSize * index + group.size}.")
        for {
          newJsons <- Future.sequence(
            group.map { case (id, replacedJsValue) =>
              repo.get(id).map { json =>
                json.get ++ Json.obj(spec.fieldName -> replacedJsValue)
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
}

case class ReplaceZeroWithNullSpec(dataSetId: String, fieldName: String, batchSize: Int)