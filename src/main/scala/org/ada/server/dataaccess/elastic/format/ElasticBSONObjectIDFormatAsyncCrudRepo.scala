package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.http.HttpClient
import org.incal.access.elastic.ElasticSetting
import org.incal.core.Identity
import play.api.libs.json.Format
import reactivemongo.bson.BSONObjectID
import ElasticIdRenameUtil.wrapFormat

import scala.reflect.runtime.universe.TypeTag

final class ElasticBSONObjectIDFormatAsyncCrudRepo[E, ID](
  indexName: String,
  typeName: String,
  val client: HttpClient,
  setting: ElasticSetting)(
  implicit typeTag: TypeTag[E], manifest: Manifest[E], coreFormat: Format[E], identity: Identity[E, ID]
) extends ElasticFormatAsyncRepo[E, ID](
  indexName, typeName, setting
) {
  override implicit val format = wrapFormat(coreFormat)

  override protected def toDBValue(value: Any): Any =
    value match {
      case b: BSONObjectID => b.stringify
      case _ => super.toDBValue(value)
    }

  override protected def toDBFieldName(fieldName: String) =
    ElasticIdRenameUtil.rename(fieldName)

  override protected def unrename(fieldName: String) =
    ElasticIdRenameUtil.unrename(fieldName)
}