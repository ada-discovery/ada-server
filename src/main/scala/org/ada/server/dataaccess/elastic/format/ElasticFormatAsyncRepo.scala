package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.indexes.IndexDefinition
import org.incal.access.elastic.{ElasticAsyncRepo, ElasticSetting}
import org.incal.core.Identity
import play.api.libs.json.Format

import scala.reflect.runtime.universe.TypeTag

abstract class ElasticFormatAsyncRepo[E, ID](
  indexName: String,
  typeName: String,
  setting: ElasticSetting)(
  implicit val format: Format[E], val manifest: Manifest[E], val typeTag: TypeTag[E], identity: Identity[E, ID]
) extends ElasticAsyncRepo[E, ID](indexName, typeName, setting) with ElasticFormatSerializer[E] {

  override protected def createSaveDef(entity: E, id: ID): IndexDefinition =
    indexInto(indexAndType) source entity id id
}