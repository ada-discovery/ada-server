package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.ElasticClient
import org.incal.access.elastic.ElasticSetting
import play.api.libs.json.Format
import reactivemongo.bson.BSONObjectID

import scala.reflect.runtime.universe.TypeTag

final class ElasticBSONObjectIDFormatAsyncReadonlyRepo[E, ID](
  indexName: String,
  typeName: String,
  identityName : String,
  val client: ElasticClient,
  setting: ElasticSetting)(
  implicit typeTag: TypeTag[E], coreFormat: Format[E], manifest: Manifest[E]
) extends ElasticFormatAsyncReadonlyRepo[E, ID](
  indexName, typeName, identityName, setting
) {

  override implicit val format = new ElasticIdRenameFormat(coreFormat)

  override protected def toDBValue(value: Any): Any =
    value match {
      case b: BSONObjectID => b.stringify
      case _ => super.toDBValue(value)
    }

  override protected def toDBFieldName(fieldName: String) =
    ElasticIdRenameUtil.rename(fieldName, true)

  override protected def unrename(fieldName: String) =
    ElasticIdRenameUtil.unrename(fieldName)
}
