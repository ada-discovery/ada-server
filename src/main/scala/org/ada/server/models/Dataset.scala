package org.ada.server.models

import java.util.{Date, UUID}

import play.api.libs.functional.syntax._
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._
import org.ada.server.dataaccess._
import org.ada.server.models.json.{EnumFormat, ManifestedFormat, SerializableFormat, SubTypeFormat}
import org.incal.core.Identity

import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

case class DataSpaceMetaInfo(
  _id: Option[BSONObjectID],
  name: String,
  sortOrder: Int,
  timeCreated: Date = new Date(),
  dataSetMetaInfos: Seq[DataSetMetaInfo] = Seq[DataSetMetaInfo](),
  parentId: Option[BSONObjectID] = None,
  var children: Buffer[DataSpaceMetaInfo] = ListBuffer[DataSpaceMetaInfo]()
)

case class DataSetMetaInfo(
  _id: Option[BSONObjectID],
  id: String,
  name: String,
  sortOrder: Int,
  hide: Boolean,
  dataSpaceId: BSONObjectID,
  timeCreated: Date = new Date(),
  sourceDataSetId: Option[BSONObjectID] = None
)

object StorageType extends Enumeration {
  val Mongo = Value("Mongo")
  val ElasticSearch = Value("Elastic Search")
}

case class DataSetSetting(
  _id: Option[BSONObjectID],
  dataSetId: String,
  keyFieldName: String,
  exportOrderByFieldName: Option[String] = None,
  defaultScatterXFieldName: Option[String] = None,
  defaultScatterYFieldName: Option[String] = None,
  defaultDistributionFieldName: Option[String] = None,
  defaultCumulativeCountFieldName: Option[String] = None,
  filterShowFieldStyle: Option[FilterShowFieldStyle.Value] = None,
  filterShowNonNullCount: Boolean = false,
  displayItemName: Option[String] = None,
  storageType: StorageType.Value,
  mongoAutoCreateIndexForProjection: Boolean = false,
  cacheDataSet: Boolean = false
) {
  def this(
    dataSetId: String,
    storageType: StorageType.Value
  ) =
    this(_id = None, dataSetId = dataSetId, keyFieldName = "_id", storageType = storageType)

  def this(dataSetId: String) =
    this(dataSetId, StorageType.Mongo)
}

case class Dictionary(
  _id: Option[BSONObjectID],
  dataSetId: String,
  fields: Seq[Field],
  categories: Seq[Category],
  filters: Seq[Filter],
  dataviews: Seq[DataView]
//  classificationResults: Seq[ClassificationResult]
//  parents : Seq[Dictionary],
)

case class Field(
  name: String,
  label: Option[String] = None,
  fieldType: FieldTypeId.Value = FieldTypeId.String,
  isArray: Boolean = false,
  numValues: Option[Map[String, String]] = None, // TODO: rename to enumValues // also rename FieldCacheCrudRepoFactory fieldsToExclude
  displayDecimalPlaces: Option[Int] = None,
  displayTrueValue: Option[String] = None,
  displayFalseValue: Option[String] = None,
  aliases: Seq[String] = Seq[String](),
  var categoryId: Option[BSONObjectID] = None,
  var category: Option[Category] = None
) {
  def fieldTypeSpec: FieldTypeSpec =
    FieldTypeSpec(
      fieldType,
      isArray,
      numValues.map(_.map{ case (a,b) => (a.toInt, b)}),
      displayDecimalPlaces,
      displayTrueValue,
      displayFalseValue
    )

  def labelOrElseName = label.getOrElse(name)
}

object FieldTypeId extends Enumeration {
  val Null, Boolean, Double, Integer, Enum, String, Date, Json = Value
}

case class FieldTypeSpec(
  fieldType: FieldTypeId.Value,
  isArray: Boolean = false,
  enumValues: Option[Map[Int, String]] = None,
  displayDecimalPlaces: Option[Int] = None,
  displayTrueValue: Option[String] = None,
  displayFalseValue: Option[String] = None
)

case class NumFieldStats(min : Double, max : Double, mean : Double, variance : Double)

case class Category(
  _id: Option[BSONObjectID],
  name: String,
  label: Option[String] = None,
  var parentId: Option[BSONObjectID] = None,
  var parent: Option[Category] = None,
  var children: Seq[Category] = Seq[Category]()
) {
  def this(name : String) = this(None, name)

  def getPath: Seq[String] =
    (
      if (parent.isDefined && parent.get.parent.isDefined)
        parent.get.getPath
      else
        Seq[String]()
    ) ++ Seq(name)

  def getLabelPath: Seq[String] =
    (
      if (parent.isDefined && parent.get.parent.isDefined)
        parent.get.getLabelPath
      else
        Seq[String]()
    ) ++ Seq(labelOrElseName)

  def setChildren(newChildren: Seq[Category]): Category = {
    children = newChildren
    children.foreach(_.parent = Some(this))
    this
  }

  def addChild(newChild: Category): Category = {
    children = Seq(newChild) ++ children
    newChild.parent = Some(this)
    this
  }

  def labelOrElseName = label.getOrElse(name)

  override def toString = name

  override def hashCode = name.hashCode
}

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
    (__ \ "numValues").formatNullable[Map[String, String]] and
    (__ \ "displayDecimalPlaces").formatNullable[Int] and
    (__ \ "displayTrueValue").formatNullable[String] and
    (__ \ "displayFalseValue").formatNullable[String] and
    (__ \ "aliases").format[Seq[String]] and
    (__ \ "categoryId").formatNullable[BSONObjectID]
  )(
    Field(_, _, _, _, _, _, _, _, _, _),
    (item: Field) =>  (
        item.name, item.label, item.fieldType, item.isArray, item.numValues, item.displayDecimalPlaces,
        item.displayTrueValue, item.displayFalseValue, item.aliases, item.categoryId
      )
  )

  implicit val chartTypeFormat = EnumFormat(ChartType)
  implicit val aggTypeFormat = EnumFormat(AggType)
  implicit val correlationTypeFormat = EnumFormat(CorrelationType)
  implicit val basicDisplayOptionsFormat = Json.format[BasicDisplayOptions]
  implicit val distributionDisplayOptionsFormat = Json.format[MultiChartDisplayOptions]

  // register your widget spec class here
  private val widgetSpecManifestedFormats: Seq[ManifestedFormat[_ <: WidgetSpec]] =
    Seq(
      ManifestedFormat(Json.format[DistributionWidgetSpec]),
      ManifestedFormat(Json.format[CumulativeCountWidgetSpec]),
      ManifestedFormat(Json.format[BoxWidgetSpec]),
      ManifestedFormat(Json.format[ScatterWidgetSpec]),
      ManifestedFormat(Json.format[ValueScatterWidgetSpec]),
      ManifestedFormat(Json.format[HeatmapAggWidgetSpec]),
      ManifestedFormat(Json.format[GridDistributionCountWidgetSpec]),
      ManifestedFormat(Json.format[CorrelationWidgetSpec]),
      ManifestedFormat(Json.format[IndependenceTestWidgetSpec]),
      ManifestedFormat(Json.format[BasicStatsWidgetSpec]),
      ManifestedFormat(Json.format[CustomHtmlWidgetSpec])
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