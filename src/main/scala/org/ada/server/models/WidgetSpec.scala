package org.ada.server.models

import reactivemongo.bson.BSONObjectID

abstract class WidgetSpec {
  val fieldNames: Traversable[String]
  val subFilterId: Option[BSONObjectID]
  val displayOptions: DisplayOptions
}

sealed trait DisplayOptions {
  val gridWidth: Option[Int]
  val gridOffset: Option[Int]
  val height: Option[Int]
  val isTextualForm: Boolean
  val title: Option[String]
}

case class BasicDisplayOptions(
  gridWidth: Option[Int] = None,
  gridOffset: Option[Int] = None,
  height: Option[Int] = None,
  isTextualForm: Boolean = false,
  title: Option[String] = None
) extends DisplayOptions

object ChartType extends Enumeration {
  val Pie, Column, Bar, Line, Spline, Polar = Value
}

case class MultiChartDisplayOptions(
  gridWidth: Option[Int] = None,
  gridOffset: Option[Int] = None,
  height: Option[Int] = None,
  chartType: Option[ChartType.Value] = None,
  isTextualForm: Boolean = false,
  title: Option[String] = None
) extends DisplayOptions

case class DistributionWidgetSpec(
  fieldName: String,
  groupFieldName: Option[String],
  subFilterId: Option[BSONObjectID] = None,
  relativeValues: Boolean = false,
  numericBinCount: Option[Int] = None, // TODO: rename to binCount
  useDateMonthBins: Boolean = false,
  displayOptions: MultiChartDisplayOptions = MultiChartDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(groupFieldName, Some(fieldName)).flatten
}

case class CategoricalCheckboxWidgetSpec(
  fieldName: String,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(fieldName)
}

case class CumulativeCountWidgetSpec(
  fieldName: String,
  groupFieldName: Option[String],
  subFilterId: Option[BSONObjectID] = None,
  relativeValues: Boolean = false,
  numericBinCount: Option[Int] = None,
  useDateMonthBins: Boolean = false,
  displayOptions: MultiChartDisplayOptions = MultiChartDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(groupFieldName, Some(fieldName)).flatten
}

case class BoxWidgetSpec(
  fieldName: String,
  groupFieldName: Option[String] = None,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(groupFieldName, Some(fieldName)).flatten
}

case class ScatterWidgetSpec(
  xFieldName: String,
  yFieldName: String,
  groupFieldName: Option[String],
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(groupFieldName, Some(xFieldName), Some(yFieldName)).flatten
}

case class ValueScatterWidgetSpec(
  xFieldName: String,
  yFieldName: String,
  valueFieldName: String,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(xFieldName, yFieldName, valueFieldName)
}

object AggType extends Enumeration {
  val Mean, Max, Min, Variance = Value
}

case class HeatmapAggWidgetSpec(
  xFieldName: String,
  yFieldName: String,
  valueFieldName: String,
  xBinCount: Int,
  yBinCount: Int,
  aggType: AggType.Value,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(xFieldName, yFieldName, valueFieldName)
}

case class GridDistributionCountWidgetSpec(
  xFieldName: String,
  yFieldName: String,
  xBinCount: Int,
  yBinCount: Int,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(xFieldName, yFieldName)
}

object CorrelationType extends Enumeration {
  val Pearson, Matthews = Value
}

case class CorrelationWidgetSpec(
  fieldNames: Seq[String],
  correlationType: CorrelationType.Value,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec

case class IndependenceTestWidgetSpec(
  fieldName: String,
  inputFieldNames: Seq[String],
  topCount: Option[Int] = None,
  keepUndefined: Boolean = false,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(fieldName) ++ inputFieldNames
}

case class BasicStatsWidgetSpec(
  fieldName: String,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Seq(fieldName)
}

case class CustomHtmlWidgetSpec(
  content: String,
  subFilterId: Option[BSONObjectID] = None,
  displayOptions: BasicDisplayOptions = BasicDisplayOptions()
) extends WidgetSpec {
  override val fieldNames = Nil
}