package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.ada.server.dataaccess.RepoTypes.RegressorRepo
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import reactivemongo.bson.BSONObjectID
import org.ada.server.services.ml.MachineLearningService
import org.incal.core.runnables.InputFutureRunnableExt
import org.incal.core.dataaccess.Criterion.Infix
import org.incal.core.dataaccess.NotEqualsNullCriterion
import org.incal.spark_ml.models.setting.{TemporalGroupIOSpec, TemporalRegressionLearningSetting}
import org.ada.server.field.FieldUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf
import scala.concurrent.Future

class RunRowTimeSeriesDLRegression @Inject() (
    dsaf: DataSetAccessorFactory,
    mlService: MachineLearningService,
    regressionRepo: RegressorRepo
  ) extends InputFutureRunnableExt[RunRowTimeSeriesDLRegressionSpec] with TimeSeriesResultsHelper {

  private val ftf = FieldTypeHelper.fieldTypeFactory()

  override def runAsFuture(
    input: RunRowTimeSeriesDLRegressionSpec
  ): Future[Unit] = {
    val dsa = dsaf(input.dataSetId).get

    val ioSpec = input.ioSpec
    val fieldNames = ioSpec.allFieldNames

    for {
      // load a ML model
      mlModel <- regressionRepo.get(input.mlModelId)

      // get all the fields
      fields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> fieldNames))
      fieldNameSpecs = fields.map(field => (field.name, field.fieldTypeSpec)).toSeq

      // order field (and type)
      orderField <- dsa.fieldRepo.get(ioSpec.orderFieldName).map(_.get)
      orderFieldType = ftf(orderField.fieldTypeSpec).asValueOf[Any]
      orderedValues = ioSpec.orderedStringValues.map(x => orderFieldType.displayStringToValue(x).get)

      // group id field (and type)
      groupIdField <- dsa.fieldRepo.get(ioSpec.groupIdFieldName).map(_.get)
      groupIdFieldType = ftf(groupIdField.fieldTypeSpec).asValueOf[Any]

      // filter criteria
      filterCriteria <- loadCriteria(dsa, ioSpec.filterId)

      // not null field criteria
      notNullFieldCriteria = fields.map(field => NotEqualsNullCriterion(field.name))

      // jsons
      data <- dsa.dataSetRepo.find(
        criteria = filterCriteria ++ notNullFieldCriteria,
        projection = fieldNames
      )

      // run the selected classifier (ML model)
      resultsHolder <- mlModel.map { mlModel =>
        val results = mlService.regressRowTemporalSeries(
          data,
          fieldNameSpecs,
          ioSpec.inputFieldNames,
          ioSpec.outputFieldName,
          ioSpec.orderFieldName,
          orderedValues,
          Some(ioSpec.groupIdFieldName),
          mlModel,
          input.learningSetting
        )
        results.map(Some(_))
      }.getOrElse(
        Future(None)
      )
    } yield
      resultsHolder.foreach(exportResults)
  }

  private def loadCriteria(dsa: DataSetAccessor, filterId: Option[BSONObjectID]) =
    for {
      filter <- filterId match {
        case Some(filterId) => dsa.filterRepo.get(filterId)
        case None => Future(None)
      }

      criteria <- filter match {
        case Some(filter) => FieldUtil.toDataSetCriteria(dsa.fieldRepo, filter.conditions)
        case None => Future(Nil)
      }
    } yield
      criteria
}

case class RunRowTimeSeriesDLRegressionSpec(
  dataSetId: String,
  ioSpec: TemporalGroupIOSpec,
  mlModelId: BSONObjectID,
  learningSetting: TemporalRegressionLearningSetting
)