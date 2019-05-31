package org.ada.server.runnables.core

import play.api.Logger
import runnables.DsaInputFutureRunnable
import org.ada.server.models._

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class CreateDefaultMainView extends DsaInputFutureRunnable[CreateDefaultMainViewSpec] {

  override def runAsFuture(input: CreateDefaultMainViewSpec) = {
    val dsa_ = createDsa(input.dataSetId)
    val fieldRepo = dsa_.fieldRepo
    val viewRepo = dsa_.dataViewRepo

    for {
      // get the fields
      fields <- fieldRepo.find()

      // add rounding for the double fields (if needed) and introduce a default label
      _ <- {
        val newFields = fields.map { field =>
          field.fieldType match {
            case FieldTypeId.Double => field.copy(label = Some(field.name), displayDecimalPlaces = input.doubleDecimalPlaces)
            case _ => field.copy(label = Some(field.name))
          }
        }
        fieldRepo.update(newFields)
      }

      // create and save the main view
      _ <- viewRepo.save(mainDataView(fields, input))
    } yield
      ()
  }

  private def mainDataView(fields: Traversable[Field], spec: CreateDefaultMainViewSpec): DataView = {
    val doubleFieldNames = fields.filter(_.fieldType == FieldTypeId.Double).map(_.name).toSeq.sorted
    val nonDoubleFieldNames = fields.filter(_.fieldType != FieldTypeId.Double).map(_.name).toSeq.sorted

    val distributionDisplayOptions = MultiChartDisplayOptions(
      chartType = Some(ChartType.Column),
      gridWidth = spec.distributionWidgetGridWidth
    )

    val distributionWidgets = doubleFieldNames.map(
      DistributionWidgetSpec(_, None, displayOptions = distributionDisplayOptions)
    )

    val boxPlotWidgets = doubleFieldNames.map(
      BoxWidgetSpec(_, None, displayOptions = BasicDisplayOptions(gridWidth = spec.boxWidgetGridWidth))
    )

    val correlationWidget = CorrelationWidgetSpec(
      fieldNames = doubleFieldNames,
      correlationType = CorrelationType.Pearson,
      displayOptions = BasicDisplayOptions(gridWidth = spec.correlationWidgetGridWidth)
    )

    def randomDoubleFieldName: String =
      doubleFieldNames(Random.nextInt(doubleFieldNames.size))

    val scatterWidget = ScatterWidgetSpec(
      randomDoubleFieldName,
      randomDoubleFieldName,
      None,
      displayOptions =  BasicDisplayOptions(gridWidth = spec.scatterWidgetGridWidth)
    )

    DataView(
      None, "Main", Nil,
      nonDoubleFieldNames ++ doubleFieldNames,
      distributionWidgets ++ boxPlotWidgets ++ Seq(correlationWidget, scatterWidget),
      spec.defaultElementGridWidth,
      true
    )
  }
}

case class CreateDefaultMainViewSpec(
  dataSetId: String,
  doubleDecimalPlaces: Option[Int],
  defaultElementGridWidth: Int,
  distributionWidgetGridWidth: Option[Int],
  boxWidgetGridWidth: Option[Int],
  correlationWidgetGridWidth: Option[Int],
  scatterWidgetGridWidth: Option[Int]
)