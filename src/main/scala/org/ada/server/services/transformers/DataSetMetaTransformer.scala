package org.ada.server.services.transformers

import akka.stream.Materializer
import javax.inject.Inject
import org.ada.server.AdaException
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import org.ada.server.services.DataSetService
import org.ada.server.util.MessageLogger
import org.ada.server.models.datatrans.{DataSetMetaTransformation, DataSetTransformation, ResultDataSetSpec}
import org.incal.core.runnables.InputFutureRunnable
import play.api.Logger

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DataSetMetaTransformer[T <: DataSetMetaTransformation] extends InputFutureRunnable[T]

private[transformers] abstract class AbstractDataSetMetaTransformer[T <: DataSetMetaTransformation](implicit val typeTag: TypeTag[T]) extends DataSetMetaTransformer[T] {

  @Inject var messageRepo: MessageRepo = _
  @Inject var dataSetService: DataSetService = _
  @Inject var dsaf: DataSetAccessorFactory = _
  @Inject implicit var materializer: Materializer = _

  protected val logger = Logger
  protected lazy val messageLogger = MessageLogger(logger, messageRepo)

  protected val defaultFti = FieldTypeHelper.fieldTypeInferrer
  protected val ftf = FieldTypeHelper.fieldTypeFactory()
  protected val defaultCharset = "UTF-8"
  protected val metaDeleteAndSave = true
  protected val deleteNonReferenced = true

  protected def dsaSafe(dataSetId: String) =
    dsaf(dataSetId).getOrElse(throw new AdaException(s"Data set ${dataSetId} not found"))

  override def runAsFuture(spec: T) =
    for {
      // execute the transformation (internally)
      (dsa, fields, categories) <- execInternal(spec)

      // update the fields (if any)
      _ <- if (fields.nonEmpty)
          dataSetService.updateFields(dsa.fieldRepo, fields, metaDeleteAndSave, deleteNonReferenced)
        else
          Future(())

      // update the categories (if any)
      _ <- if (categories.nonEmpty)
          dataSetService.updateCategories(dsa.categoryRepo, categories, metaDeleteAndSave, deleteNonReferenced)
        else
          Future(())

    } yield
      ()

  protected def execInternal(spec: T): Future[(
    DataSetAccessor,
    Traversable[Field],
    Traversable[Category]
  )]
}