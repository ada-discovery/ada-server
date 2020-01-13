package org.ada.server.models

import java.util.UUID

import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.{EnumFormat, RuntimeClassFormat, SerializableFormat, SubTypeFormat}
import org.incal.core.Identity
import play.api.libs.functional.syntax._
import reactivemongo.play.json.BSONFormats._
import play.api.libs.json._
import play.api.libs.json.{Format, JsObject, Json}
import reactivemongo.bson.BSONObjectID
import org.ada.server.models.NavigationItem.navigationItemFormat

// JSON converters and identities

object DataSetFormattersAndIds {

  implicit val enumTypeFormat = EnumFormat(FieldTypeId)
  implicit val categoryFormat: Format[Category] = (
    (__ \ "_id").formatNullable[BSONObjectID] and
    (__ \ "name").format[String] and
    (__ \ "label").formatNullable[String] and
    (__ \ "parentId").formatNullable[BSONObjectID]
  )(Category(_, _, _, _), (item: Category) =>  (item._id, item.name, item.label, item.parentId))

  implicit val fieldFormat: Format[Field] = (
    (__ \ "name").format[String] and
    (__ \ "label").formatNullable[String] and
    (__ \ "fieldType").format[FieldTypeId.Value] and
    (__ \ "isArray").format[Boolean] and
    (__ \ "enumValues").format[Map[String, String]] and
    (__ \ "displayDecimalPlaces").formatNullable[Int] and
    (__ \ "displayTrueValue").formatNullable[String] and
    (__ \ "displayFalseValue").formatNullable[String] and
    (__ \ "aliases").format[Seq[String]] and
    (__ \ "categoryId").formatNullable[BSONObjectID]
  )(
    Field(_, _, _, _, _, _, _, _, _, _),
    (item: Field) =>  (
      item.name, item.label, item.fieldType, item.isArray, item.enumValues, item.displayDecimalPlaces,
      item.displayTrueValue, item.displayFalseValue, item.aliases, item.categoryId
    )
  )

  implicit val chartTypeFormat = EnumFormat(ChartType)
  implicit val aggTypeFormat = EnumFormat(AggType)
  implicit val correlationTypeFormat = EnumFormat(CorrelationType)
  implicit val basicDisplayOptionsFormat = Json.format[BasicDisplayOptions]
  implicit val distributionDisplayOptionsFormat = Json.format[MultiChartDisplayOptions]

  // register your widget spec class here
  private val widgetSpecManifestedFormats: Seq[RuntimeClassFormat[_ <: WidgetSpec]] =
    Seq(
      RuntimeClassFormat(Json.format[DistributionWidgetSpec]),
      RuntimeClassFormat(Json.format[CumulativeCountWidgetSpec]),
      RuntimeClassFormat(Json.format[BoxWidgetSpec]),
      RuntimeClassFormat(Json.format[ScatterWidgetSpec]),
      RuntimeClassFormat(Json.format[ValueScatterWidgetSpec]),
      RuntimeClassFormat(Json.format[HeatmapAggWidgetSpec]),
      RuntimeClassFormat(Json.format[GridDistributionCountWidgetSpec]),
      RuntimeClassFormat(Json.format[CorrelationWidgetSpec]),
      RuntimeClassFormat(Json.format[IndependenceTestWidgetSpec]),
      RuntimeClassFormat(Json.format[BasicStatsWidgetSpec]),
      RuntimeClassFormat(Json.format[CustomHtmlWidgetSpec]),
      RuntimeClassFormat(Json.format[CategoricalCheckboxWidgetSpec])
    )

  val widgetSpecClasses: Seq[Class[_ <: WidgetSpec]] = widgetSpecManifestedFormats.map(_.runtimeClass)

  implicit val widgetSpecFormat: Format[WidgetSpec] = new SubTypeFormat[WidgetSpec](widgetSpecManifestedFormats)

  implicit val dictionaryFormat = Json.format[Dictionary]
  implicit val dataSetMetaInfoFormat = Json.format[DataSetMetaInfo]

  val dataSpaceMetaInfoFormat: Format[DataSpaceMetaInfo] = (
    (__ \ "_id").formatNullable[BSONObjectID] and
    (__ \ "name").format[String] and
    (__ \ "sortOrder").format[Int] and
    (__ \ "timeCreated").format[java.util.Date] and
    (__ \ "dataSetMetaInfos").format[Seq[DataSetMetaInfo]] and
    (__ \ "parentId").formatNullable[BSONObjectID]
  )(
    DataSpaceMetaInfo(_, _, _, _, _, _),
    (item: DataSpaceMetaInfo) =>  (
      item._id, item.name, item.sortOrder, item.timeCreated, item.dataSetMetaInfos, item.parentId
    )
  )
  implicit val serializableDataSpaceMetaInfoFormat: Format[DataSpaceMetaInfo] = new SerializableFormat(dataSpaceMetaInfoFormat.reads, dataSpaceMetaInfoFormat.writes)

  implicit val filterShowFieldStyleFormat = EnumFormat(FilterShowFieldStyle)
  implicit val storageTypeFormat = EnumFormat(StorageType)

  val dataSetSettingFormat = Json.format[DataSetSetting]
  implicit val serializableDataSetSettingFormat = new SerializableFormat(dataSetSettingFormat.reads, dataSetSettingFormat.writes)

  val serializableBSONObjectIDFormat = new SerializableFormat(BSONObjectIDFormat.reads, BSONObjectIDFormat.writes)

  implicit object DictionaryIdentity extends BSONObjectIdentity[Dictionary] {
    def of(entity: Dictionary): Option[BSONObjectID] = entity._id
    protected def set(entity: Dictionary, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  implicit object FieldIdentity extends Identity[Field, String] {
    override val name = "name"
    override def next = UUID.randomUUID().toString
    override def set(entity: Field, name: Option[String]): Field = entity.copy(name = name.getOrElse(""))
    override def of(entity: Field): Option[String] = Some(entity.name)
  }

  implicit object CategoryIdentity extends BSONObjectIdentity[Category] {
    def of(entity: Category): Option[BSONObjectID] = entity._id
    protected def set(entity: Category, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  implicit object DataSetMetaInfoIdentity extends BSONObjectIdentity[DataSetMetaInfo] {
    def of(entity: DataSetMetaInfo): Option[BSONObjectID] = entity._id
    protected def set(entity: DataSetMetaInfo, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  implicit object DataSpaceMetaInfoIdentity extends BSONObjectIdentity[DataSpaceMetaInfo] {
    def of(entity: DataSpaceMetaInfo): Option[BSONObjectID] = entity._id
    protected def set(entity: DataSpaceMetaInfo, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  implicit object DataSetSettingIdentity extends BSONObjectIdentity[DataSetSetting] {
    def of(entity: DataSetSetting): Option[BSONObjectID] = entity._id
    protected def set(entity: DataSetSetting, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  implicit object JsObjectIdentity extends BSONObjectIdentity[JsObject] {
    override def of(json: JsObject): Option[BSONObjectID] =
      (json \ name).toOption.flatMap(_.asOpt[BSONObjectID])

    override protected def set(json: JsObject, id: Option[BSONObjectID]): JsObject =
      json.+(name, Json.toJson(id))
  }
}
