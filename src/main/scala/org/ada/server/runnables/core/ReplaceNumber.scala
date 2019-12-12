package org.ada.server.runnables.core

import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import org.ada.server.AdaException
import play.api.libs.json._
import runnables.DsaInputFutureRunnable
import org.ada.server.field.FieldUtil.FieldOps
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import org.incal.core.dataaccess.CrudRepoExtra._
import org.incal.core.dataaccess.StreamSpec
import org.incal.core.dataaccess.NotEqualsNullCriterion

import scala.concurrent.ExecutionContext.Implicits.global

class ReplaceNumber extends DsaInputFutureRunnable[ReplaceNumberSpec] {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val flatFlow = Flow[Option[JsObject]].collect { case Some(x) => x }

  override def runAsFuture(spec: ReplaceNumberSpec) = {
    val dsa = createDsa(spec.dataSetId)

    for {
      // field
      fieldOption <- dsa.fieldRepo.get(spec.fieldName)
      field = fieldOption.getOrElse(throw new AdaException(s"Field ${spec.fieldName} not found."))

      // replace for numbers or eum
      _ <- if (!field.isArray && (field.isNumeric || field.isEnum))
          replaceNumber(dsa.dataSetRepo, spec)
        else
          throw new AdaException(s"Number replacement is possible only for double, integer, date, an enum types but got ${field.fieldTypeSpec}.")
    } yield
      ()
  }

  private def replaceNumber(
    repo: JsonCrudRepo,
    spec: ReplaceNumberSpec
  ) =
    for {
      // input stream
      inputStream <- repo.findAsStream(Seq(NotEqualsNullCriterion(spec.fieldName)))

      // replaced stream
      replacedStream = inputStream.map( json =>
        (json \ spec.fieldName).get match {
          case JsNumber(value) if value.equals(spec.from) => Some(json.+(spec.fieldName, JsNumber(spec.to)))
          case _ =>  None
        }
      )

      // update the replaced jsons as stream
      _ <- repo.updateAsStream(replacedStream.via(flatFlow), spec.updateStreamSpec)
    } yield
      ()
}

case class ReplaceNumberSpec(
  dataSetId: String,
  fieldName: String,
  from: Double,
  to: Double,
  updateStreamSpec: StreamSpec
)