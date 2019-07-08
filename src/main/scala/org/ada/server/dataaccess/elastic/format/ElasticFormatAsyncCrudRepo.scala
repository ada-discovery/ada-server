package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.http.ElasticDsl
import org.incal.access.elastic.{ElasticAsyncCrudRepo, ElasticSetting}
import org.incal.core.Identity
import play.api.libs.json.Format

import scala.reflect.runtime.universe.TypeTag

abstract class ElasticFormatAsyncCrudRepo[E, ID](
  indexName: String,
  typeName: String,
  setting: ElasticSetting)(
  implicit val format: Format[E], val manifest: Manifest[E], val typeTag: TypeTag[E], identity: Identity[E, ID]
) extends ElasticAsyncCrudRepo[E, ID](indexName, typeName, setting) with ElasticFormatSerializer[E] {

  override protected def createSaveDef(entity: E, id: ID) =
    indexInto(indexAndType) source entity id id

  override def createUpdateDef(entity: E, id: ID) =
    ElasticDsl.update(id) in indexAndType source entity
}