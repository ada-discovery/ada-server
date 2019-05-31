package org.ada.server.services.transformers

import akka.stream.scaladsl.Source
import javax.inject.Inject
import org.ada.server.AdaException
import org.ada.server.field.{FieldTypeHelper, FieldTypeInferrer}
import org.ada.server.models._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import org.ada.server.services.DataSetService
import org.ada.server.util.MessageLogger
import org.ada.server.models.datatrans.{DataSetTransformation, ResultDataSetSpec}
import org.incal.core.runnables.InputFutureRunnable
import play.api.Logger
import play.api.libs.json.JsObject

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DataSetTransformer[T <: DataSetTransformation] extends InputFutureRunnable[T]

private[transformers] abstract class AbstractDataSetTransformer[T <: DataSetTransformation](implicit val typeTag: TypeTag[T]) extends DataSetTransformer[T] {

  @Inject var messageRepo: MessageRepo = _
  @Inject var dataSetService: DataSetService = _
  @Inject var dsaf: DataSetAccessorFactory = _

  protected val logger = Logger
  protected lazy val messageLogger = MessageLogger(logger, messageRepo)

  protected val defaultFti = FieldTypeHelper.fieldTypeInferrer
  protected val ftf = FieldTypeHelper.fieldTypeFactory()
  protected val defaultCharset = "UTF-8"

  protected val saveViewsAndFiltersFlag = true

  override def runAsFuture(spec: T) =
    execInternal(spec).flatMap { case (sourceDsa, fields, inputSourceOptional) =>
      inputSourceOptional.map(inputSource =>
        dataSetService.saveDerivedDataSet(
          sourceDsa,
          spec.resultDataSetSpec,
          inputSource,
          fields,
          spec.streamSpec,
          saveViewsAndFiltersFlag
        )
      ).getOrElse(
        if (spec.sourceDataSetIds.size == 1 && spec.resultDataSetId == spec.sourceDataSetIds.head) {
          dataSetService.updateDictionaryFields(spec.resultDataSetId, fields, true, true)
        } else {
          throw new AdaException(s"Transformation `${spec.getClass.getSimpleName}` with pure fields update (i.e., no data source) can be used only on the data set itself but got '${spec.resultDataSetId}' vs '${spec.sourceDataSetIds.mkString(", ")}'.")
        }
      )
    }

  protected def execInternal(spec: T): Future[(
    DataSetAccessor,
    Traversable[Field],
    Option[Source[JsObject, _]]
  )]
}