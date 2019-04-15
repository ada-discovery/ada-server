package org.ada.server.models

import play.api.libs.functional.syntax._
import reactivemongo.bson.BSONObjectID
import org.ada.server.models.DataSetFormattersAndIds.widgetSpecFormat
import java.util.Date

import org.ada.server.json.{EitherFormat, EnumFormat}
import org.ada.server.dataaccess.BSONObjectIdentity
import org.incal.core.FilterCondition
import org.ada.server.models.Filter._
import play.api.libs.json._
import reactivemongo.play.json.BSONFormats._

case class DataView(
  _id: Option[BSONObjectID],
  name: String,
  filterOrIds: Seq[Either[Seq[FilterCondition], BSONObjectID]],
  tableColumnNames: Seq[String],
  widgetSpecs: Seq[WidgetSpec],
  elementGridWidth: Int = 3,
  default: Boolean = false,
  isPrivate: Boolean = false,
  generationMethod: WidgetGenerationMethod.Value = WidgetGenerationMethod.Auto,
  createdById: Option[BSONObjectID] = None,
  timeCreated: Date = new Date(),
  var createdBy: Option[User] = None
)

object WidgetGenerationMethod extends Enumeration {

  val Auto = Value("Auto")
  val FullData = Value("Full Data")
  val StreamedAll = Value("Streamed All")
  val StreamedIndividually = Value("Streamed Individually")
  val RepoAndFullData = Value("Repo and Full Data")
  val RepoAndStreamedAll = Value("Repo and Streamed All")
  val RepoAndStreamedIndividually = Value("Repo and Streamed Individually")

  implicit class ValueExt(method: WidgetGenerationMethod.Value) {
    def isRepoBased = method == RepoAndFullData || method == RepoAndStreamedAll || method == RepoAndStreamedIndividually
  }
}

object DataView {

  implicit val eitherFormat = EitherFormat[Seq[FilterCondition], BSONObjectID]
  implicit val generationMethodFormat = EnumFormat(WidgetGenerationMethod)

  implicit val dataViewFormat : Format[DataView] = (
    (__ \ "_id").formatNullable[BSONObjectID] and
    (__ \ "name").format[String] and
    (__ \ "filterOrIds").format[Seq[Either[Seq[FilterCondition], BSONObjectID]]] and
    (__ \ "tableColumnNames").format[Seq[String]] and
    (__ \ "widgetSpecs").format[Seq[WidgetSpec]] and
    (__ \ "elementGridWidth").format[Int] and
    (__ \ "default").format[Boolean] and
    (__ \ "isPrivate").format[Boolean] and
    (__ \ "generationMethod").format[WidgetGenerationMethod.Value] and
    (__ \ "createdById").formatNullable[BSONObjectID] and
    (__ \ "timeCreated").format[Date]
  )(
    DataView(_, _, _, _, _, _, _, _, _, _, _),
    (item: DataView) =>  (
      item._id,
      item.name,
      item.filterOrIds,
      item.tableColumnNames,
      item.widgetSpecs,
      item.elementGridWidth,
      item.default,
      item.isPrivate,
      item.generationMethod,
      item.createdById,
      item.timeCreated
    )
  )

  implicit object DataViewIdentity extends BSONObjectIdentity[DataView] {
    def of(entity: DataView): Option[BSONObjectID] = entity._id
    protected def set(entity: DataView, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  def applyMain(
    tableColumnNames: Seq[String],
    distributionChartFieldNames: Seq[String],
    elementGridWidth: Int,
    generationMethod: WidgetGenerationMethod.Value
  ) =
    DataView(
      None,
      "Main",
      Nil,
      tableColumnNames,
      distributionChartFieldNames.map(DistributionWidgetSpec(_, None)),
      elementGridWidth,
      true,
      false,
      generationMethod
    )
}