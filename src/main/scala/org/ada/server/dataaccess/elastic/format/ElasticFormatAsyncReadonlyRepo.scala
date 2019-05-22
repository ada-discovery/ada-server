package org.ada.server.dataaccess.elastic.format

import org.incal.access.elastic.{ElasticAsyncReadonlyRepo, ElasticSetting}
import play.api.libs.json.Format

import scala.reflect.runtime.universe.TypeTag

abstract class ElasticFormatAsyncReadonlyRepo[E, ID](
  indexName: String,
  typeName: String,
  identityName : String,
  setting: ElasticSetting)(
  implicit val format: Format[E], val manifest: Manifest[E], val typeTag: TypeTag[E]
) extends ElasticAsyncReadonlyRepo[E, ID](indexName, typeName, identityName, setting) with ElasticFormatSerializer[E]