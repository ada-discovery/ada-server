package org.ada.server.dataaccess.elastic

import com.google.inject.assistedinject.Assisted
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.mappings.TypedFieldDefinition
import com.sksamuel.elastic4s.source.JsonDocumentSource
import org.ada.server.dataaccess.ignite.BinaryJsonUtil
import org.ada.server.dataaccess.RepoTypes.JsonCrudRepo
import org.ada.server.dataaccess.elastic.format.ElasticIdRenameUtil
import org.ada.server.models.DataSetFormattersAndIds.JsObjectIdentity
import org.ada.server.models.{FieldTypeId, FieldTypeSpec}
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONObjectIDFormat
import org.incal.access_elastic.{CoerceDoubleFieldDefinition, ElasticAsyncCrudRepo, ElasticSetting}
import javax.inject.Inject
import play.api.Configuration

class ElasticJsonCrudRepo @Inject()(
    @Assisted collectionName : String,
    @Assisted fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    val client: ElasticClient,
    configuration: Configuration
  ) extends ElasticAsyncCrudRepo[JsObject, BSONObjectID](
    collectionName,
    collectionName,
    ElasticSetting(scrollBatchSize = configuration.getInt("elastic.scroll.batch.size").getOrElse(1000))
  ) with JsonCrudRepo {

  private implicit val jsonIdRenameFormat = ElasticIdRenameUtil.createFormat
  private val fieldNamesAndTypeWithId = fieldNamesAndTypes ++ Seq((ElasticIdRenameUtil.newIdName, FieldTypeSpec(FieldTypeId.Json)))
  private val includeInAll = false

  override protected lazy val fieldDefs: Iterable[TypedFieldDefinition] =
    fieldNamesAndTypeWithId.map { case (fieldName, fieldTypeSpec) =>
      toElasticFieldType(fieldName, fieldTypeSpec)
    }

  // TODO: should be called as a post-init method, since all vals must be instantiated (i.e. the order matters)
  createIndexIfNeeded()

  override protected def serializeGetResult(response: RichGetResponse) =
    // TODO: check performance of sourceAsBytes vs sourceAsString
    Json.parse(response.sourceAsBytes) match {
      case JsNull => None
      case x: JsObject => jsonIdRenameFormat.reads(x).asOpt.map(_.asInstanceOf[JsObject])
      case _ => None
    }

  override protected def serializeSearchResult(response: RichSearchResponse) = {
    response.hits.flatMap(serializeSearchHitOptional).toIterable
//      val stringSource = hit.sourceAsString
//      val renamedStringSource = stringSource.replaceFirst("\"id\":", "\"_id\":")
//      JsObject(
//        hit.sourceAsMap.map { case (fieldName, value) =>
//          if (fieldName.equals("id"))
//            ("_id", Json.toJson(BSONObjectID.apply(value.asInstanceOf[util.HashMap[String, Any]].get("$oid").asInstanceOf[String])))
//          else
//            (fieldName, BinaryJsonUtil.toJson(value))
//        }.toSeq
//      )
  }

  override protected def serializeSearchHit(result: RichSearchHit) =
    serializeSearchHitOptional(result).getOrElse(Json.obj())

  private def serializeSearchHitOptional(result: RichSearchHit) =
    Json.parse(result.source) match {
      case x: JsObject => jsonIdRenameFormat.reads(x).asOpt.map(_.asInstanceOf[JsObject])
      case _ => None
    }

  override protected def serializeProjectionSearchResult(
    projection: Seq[String],
    result: Traversable[(String, Any)]
  ) =
    JsObject(
      result.map { case (fieldName, value) =>
        if (fieldName.startsWith(ElasticIdRenameUtil.newIdName))
          (ElasticIdRenameUtil.originalIdName, Json.toJson(BSONObjectID.parse(value.asInstanceOf[String]).get))
        else
          (fieldName, BinaryJsonUtil.toJson(value))
      }.toSeq
    )

  override protected def createSaveDef(
    entity: JsObject,
    id: BSONObjectID
  ) = {
    val stringSource = Json.stringify(jsonIdRenameFormat.writes(entity))
    index into indexAndType source stringSource id id
  }

  override def createUpdateDef(
    entity: JsObject,
    id: BSONObjectID
  ) = {
    val stringSource = Json.stringify(jsonIdRenameFormat.writes(entity))
    ElasticDsl.update id id in indexAndType doc JsonDocumentSource(stringSource)
  }

  private def toElasticFieldType(fieldName: String, fieldTypeSpec: FieldTypeSpec): TypedFieldDefinition =
    if (fieldName.equals(ElasticIdRenameUtil.newIdName)) {
      fieldName typed NestedType as ("$oid" typed StringType) //  store true
    } else
      fieldTypeSpec.fieldType match {
        case FieldTypeId.Integer => fieldName typed LongType store true includeInAll(includeInAll)
        case FieldTypeId.Double => new CoerceDoubleFieldDefinition(fieldName) store true includeInAll(includeInAll)
        case FieldTypeId.Boolean => fieldName typed BooleanType store true includeInAll(includeInAll)
        case FieldTypeId.Enum => fieldName typed IntegerType store true includeInAll(includeInAll)
        case FieldTypeId.String => fieldName typed StringType index NotAnalyzed store true includeInAll(includeInAll)
        case FieldTypeId.Date => fieldName typed LongType store true includeInAll(includeInAll)
        case FieldTypeId.Json => fieldName typed NestedType
        case FieldTypeId.Null => fieldName typed ShortType includeInAll(includeInAll) // doesn't matter which type since it's always null
      }

  override protected def toDBValue(value: Any): Any =
    value match {
      case b: BSONObjectID => b.stringify
      case _ => super.toDBValue(value)
    }

  override protected def toDBFieldName(fieldName: String) =
    ElasticIdRenameUtil.rename(fieldName, true)
}