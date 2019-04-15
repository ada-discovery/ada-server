package org.ada.server.models.ml

case class IOJsonTimeSeriesSpec(
  inputSeriesFieldPaths: Seq[String],
  outputSeriesFieldPath: String,
  dropLeftLength: Option[Int] = None,
  dropRightLength: Option[Int] = None,
  seriesLength: Option[Int] = None
)