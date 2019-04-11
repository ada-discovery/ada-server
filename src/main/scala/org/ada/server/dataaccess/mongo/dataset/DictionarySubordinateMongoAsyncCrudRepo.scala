package org.ada.server.dataaccess.mongo.dataset

import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.incal.core.dataaccess.Criterion.Infix
import org.ada.server.models.{DataSetFormattersAndIds, Dictionary}
import DataSetFormattersAndIds.{DictionaryIdentity, dictionaryFormat}
import org.ada.server.dataaccess._
import org.ada.server.dataaccess.mongo.SubordinateObjectMongoAsyncCrudRepo
import org.incal.core.Identity

import scala.concurrent.ExecutionContext.Implicits.global
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

class DictionarySubordinateMongoAsyncCrudRepo[E: Format, ID: Format](
    listName: String,
    dataSetId: String,
    dictionaryRepo: DictionaryRootRepo)(
    implicit identity: Identity[E, ID]
  ) extends SubordinateObjectMongoAsyncCrudRepo[E, ID, Dictionary, BSONObjectID](listName, dictionaryRepo) {

  override protected def getDefaultRoot =
    Dictionary(None, dataSetId, Nil, Nil, Nil, Nil)

  override protected def getRootObject =
    dictionaryRepo.find(Seq("dataSetId" #== dataSetId)).map(_.headOption)
}
