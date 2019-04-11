package org.ada.server.dataaccess

import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.models.FieldTypeSpec

trait JsonCrudRepoFactory {
  def apply(
    collectionName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)]
  ): JsonCrudRepo
}

trait MongoJsonCrudRepoFactory {
  def apply(
    collectionName: String,
    fieldNamesAndTypes: Seq[(String, FieldTypeSpec)],
    createIndexForProjectionAutomatically: Boolean
  ): JsonCrudRepo
}