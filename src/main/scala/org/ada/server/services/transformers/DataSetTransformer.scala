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
import org.ada.server.models.datatrans.{DataSetMetaTransformation, ResultDataSetSpec, DataSetTransformation}
import org.incal.core.runnables.InputFutureRunnable
import play.api.Logger
import play.api.libs.json.JsObject

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DataSetTransformer[T <: DataSetTransformation] extends DataSetMetaTransformer[T]

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
    execInternal(spec).flatMap { case (sourceDsa, fields, inputSource) =>
      dataSetService.saveDerivedDataSet(
        sourceDsa,
        spec.resultDataSetSpec,
        inputSource,
        fields,
        spec.streamSpec,
        saveViewsAndFiltersFlag
      )
    }

  protected def execInternal(spec: T): Future[(
    DataSetAccessor,
    Traversable[Field],
    Source[JsObject, _]
  )]
}