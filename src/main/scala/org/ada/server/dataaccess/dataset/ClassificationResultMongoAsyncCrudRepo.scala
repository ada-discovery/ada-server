package org.ada.server.dataaccess.dataset

import javax.inject.Inject
import com.google.inject.assistedinject.Assisted
import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.ada.server.dataaccess.mongo.dataset.DictionarySubordinateMongoAsyncCrudRepo
import org.incal.spark_ml.models.result.ClassificationResult
import org.ada.server.models.ml.classification.ClassificationResult.{ClassificationResultIdentity, classificationResultFormat}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._

import scala.concurrent.Future

class ClassificationResultMongoAsyncCrudRepo @Inject()(
    @Assisted dataSetId : String,
    dictionaryRepo: DictionaryRootRepo
  ) extends DictionarySubordinateMongoAsyncCrudRepo[ClassificationResult, BSONObjectID]("classificationResults", dataSetId, dictionaryRepo) {

  private val identity = ClassificationResultIdentity

  override def save(entity: ClassificationResult): Future[BSONObjectID] = {
    val initializedId = identity.of(entity).getOrElse(BSONObjectID.generate)
    super.save(identity.set(entity, initializedId))
  }
}