package org.ada.server.dataaccess

import org.ada.server.dataaccess.mongo.MongoAsyncCrudExtraRepo
import org.ada.server.models._
import play.api.libs.json.JsObject
import reactivemongo.bson.BSONObjectID
import org.incal.core.dataaccess._

object RepoTypes {
  type JsonReadonlyRepo = AsyncReadonlyRepo[JsObject, BSONObjectID]
  type JsonCrudRepo = AsyncCrudRepo[JsObject, BSONObjectID]

  type DictionaryRootRepo = MongoAsyncCrudExtraRepo[Dictionary, BSONObjectID]

  type FieldRepo = AsyncCrudRepo[Field, String]
  type CategoryRepo = AsyncCrudRepo[Category, BSONObjectID]
  type FilterRepo = AsyncCrudRepo[Filter, BSONObjectID]
  type DataViewRepo = AsyncCrudRepo[DataView, BSONObjectID]

  type DataSetMetaInfoRepo = AsyncCrudRepo[DataSetMetaInfo, BSONObjectID]
  type DataSpaceMetaInfoRepo = AsyncCrudRepo[DataSpaceMetaInfo, BSONObjectID]

  type DataSetSettingRepo = AsyncCrudRepo[DataSetSetting, BSONObjectID]

  type UserRepo = AsyncCrudRepo[User, BSONObjectID]
}