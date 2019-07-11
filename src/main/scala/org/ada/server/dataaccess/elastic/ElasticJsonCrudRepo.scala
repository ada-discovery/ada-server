package org.ada.server.dataaccess.elastic

import com.google.inject.assistedinject.Assisted
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.{ElasticDsl, HttpClient}
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.mappings.FieldDefinition
import org.ada.server.dataaccess.ignite.BinaryJsonUtil
import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import org.ada.server.dataaccess.elastic.format.ElasticIdRenameUtil._
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONObjectIDFormat
import org.incal.access.elastic.{ElasticAsyncCrudRepo, ElasticSetting}
import javax.inject.Inject
import play.api.Configuration

class ElasticJsonCrudRepo @Inject()(
    @Assisted("indexName") indexName : String,
    @Assisted("typeName") typeName : String,
    @Assisted("fieldNamesAndTypes") fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    @Assisted("setting") setting: Option[ElasticSetting],
    @Assisted("excludeIdMapping") excludeIdMapping: Boolean, // TODO: Derelease after migration
    val client: HttpClient,
    val configuration: Configuration
  ) extends ElasticAsyncCrudRepo[JsObject, BSONObjectID](
    indexName,
    typeName,
    setting.getOrElse(
      ElasticSetting(
        useDocScrollSort = configuration.getBoolean("elastic.scroll.doc_sort.use").getOrElse(true),
        scrollBatchSize = configuration.getInt("elastic.scroll.batch.size").getOrElse(1000),
        indexFieldsLimit = configuration.getInt("elastic.index.fields.limit").getOrElse(10000),
        shards = configuration.getInt("elastic.index.shards.num").getOrElse(5),
        replicas = configuration.getInt("elastic.index.replicas.num").getOrElse(0)
      )
    )
  ) with JsonCrudRepo {

  private val fieldNamesAndTypeWithId = fieldNamesAndTypes ++ (
    if (excludeIdMapping) Nil else Seq((storedIdName, FieldTypeSpec(FieldTypeId.String)))
  )
  private val fieldNameTypeMap = fieldNamesAndTypeWithId.toMap
  private val includeInAll = false
  private val keywordIgnoreAboveCharCount = 30000
  private val textAnalyzerName = configuration.getString("elastic.text.analyzer")

  override protected lazy val fieldDefs: Iterable[FieldDefinition] =
    fieldNamesAndTypeWithId.map { case (fieldName, fieldTypeSpec) =>
      toElasticFieldType(fieldName, fieldTypeSpec)
    }

  // TODO: should be called as a post-init method, since all vals must be instantiated (i.e. the order matters)
  createIndexIfNeeded

  override protected def serializeGetResult(response: GetResponse) =
    if (response.exists) {
      // TODO: check performance of sourceAsMap with JSON building
      Json.parse(response.sourceAsBytes) match {
        case JsNull => None
        case x: JsObject => elasticIdFormat.reads(x).asOpt.map(_.asInstanceOf[JsObject])
        case _ => None
      }
    } else
      None

  override protected def serializeSearchResult(response: SearchResponse) =
    response.hits.hits.flatMap(serializeSearchHitOptional).toIterable

  override protected def serializeSearchHit(result: SearchHit) =
    serializeSearchHitOptional(result).getOrElse(Json.obj())

  private def serializeSearchHitOptional(result: SearchHit) =
    if (result.exists) {
      // TODO: check performance of sourceAsMap with JSON building
      Json.parse(result.sourceAsBytes) match {
        case x: JsObject => elasticIdFormat.reads(x).asOpt.map(_.asInstanceOf[JsObject])
        case _ => None
      }
    } else
      None

  override protected def serializeProjectionSearchResult(
    projection: Seq[String],
    result: Traversable[(String, Any)]
  ) =
    JsObject(
      result.map { case (fieldName, value) =>

        val fieldType = fieldNameTypeMap.get(fieldName).getOrElse(
          throw new RuntimeException(s"Field $fieldName not registered for a JSON Elastic CRUD repo '$indexName'.")
        )
        val trueValue = if (!fieldType.isArray) value.asInstanceOf[Seq[Any]].head else value

        if (fieldName.equals(storedIdName))
          (originalIdName, Json.toJson(BSONObjectID.parse(trueValue.asInstanceOf[String]).get))
        else
          (fieldName, BinaryJsonUtil.toJson(trueValue))
      }.toSeq
    )

  override def stringId(id: BSONObjectID) = id.stringify

  override protected def createSaveDef(
    entity: JsObject,
    id: BSONObjectID
  ) = {
    val stringSource = Json.stringify(elasticIdFormat.writes(entity))
    indexInto(indexAndType) source stringSource id stringId(id)
  }

  override def createUpdateDef(
    entity: JsObject,
    id: BSONObjectID
  ) = {
    val stringSource = Json.stringify(elasticIdFormat.writes(entity))
    ElasticDsl.update(stringId(id)) in indexAndType doc stringSource
  }

  private def toElasticFieldType(
    fieldName: String,
    fieldTypeSpec: FieldTypeSpec
  ): FieldDefinition =
    fieldTypeSpec.fieldType match {
      case FieldTypeId.String =>
        if (textAnalyzerName.isDefined)
          textField(fieldName) store true includeInAll (includeInAll) analyzer textAnalyzerName.get
        else
          keywordField(fieldName) store true includeInAll (includeInAll) ignoreAbove keywordIgnoreAboveCharCount

      case FieldTypeId.Enum => intField(fieldName) store true includeInAll (includeInAll)
      case FieldTypeId.Boolean => booleanField(fieldName) store true includeInAll (includeInAll)
      case FieldTypeId.Integer => longField(fieldName) store true includeInAll (includeInAll)
      case FieldTypeId.Double => doubleField(fieldName) coerce true store true includeInAll (includeInAll)
      case FieldTypeId.Date => longField(fieldName) store true includeInAll (includeInAll)
      case FieldTypeId.Json => nestedField(fieldName) enabled false includeInAll (includeInAll)
      case FieldTypeId.Null => shortField(fieldName) includeInAll (includeInAll) // doesn't matter which type since it's always null
    }

  override protected def toDBValue(value: Any): Any =
    value match {
      case b: BSONObjectID => b.stringify
      case _ => super.toDBValue(value)
    }

  override protected def toDBFieldName(fieldName: String) =
    rename(fieldName)
}