package org.ada.server.dataaccess

import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.models.FieldTypeSpec
import org.incal.access.elastic.ElasticSetting
import com.google.inject.assistedinject.Assisted

trait ElasticJsonCrudRepoFactory {

  def apply(
    @Assisted("indexName") indexName : String,
    @Assisted("typeName") typeName : String,
    @Assisted("fieldNamesAndTypes") fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    @Assisted("setting") setting: Option[ElasticSetting],
    @Assisted("excludeIdMapping") excludeIdMapping: Boolean
  ): JsonCrudRepo
}

trait MongoJsonCrudRepoFactory {

  def apply(
    collectionName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    createIndexForProjectionAutomatically: Boolean
  ): JsonCrudRepo
}