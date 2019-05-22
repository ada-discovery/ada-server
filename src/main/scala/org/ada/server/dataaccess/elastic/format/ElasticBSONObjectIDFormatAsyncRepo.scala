package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.ElasticClient
import org.incal.access.elastic.ElasticSetting
import org.incal.core.Identity
import play.api.libs.json.Format
import reactivemongo.bson.BSONObjectID

import scala.reflect.runtime.universe.TypeTag

final class ElasticBSONObjectIDFormatAsyncRepo[E, ID](
  indexName: String,
  typeName: String,
  val client: ElasticClient,
  setting: ElasticSetting)(
  implicit typeTag: TypeTag[E], coreFormat: Format[E], manifest: Manifest[E], identity: Identity[E, ID]
) extends ElasticFormatAsyncCrudRepo[E, ID](
  indexName, typeName, setting
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