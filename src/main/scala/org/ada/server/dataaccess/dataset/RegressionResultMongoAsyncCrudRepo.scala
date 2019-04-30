package org.ada.server.dataaccess.dataset

import javax.inject.Inject
import com.google.inject.assistedinject.Assisted
import org.ada.server.dataaccess.RepoTypes.DictionaryRootRepo
import org.ada.server.dataaccess.mongo.dataset.DictionarySubordinateMongoAsyncCrudRepo
import org.incal.spark_ml.models.result.RegressionResult
import org.ada.server.models.ml.regression.RegressionResult.{regressionResultFormat, RegressionResultIdentity}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats._

import scala.concurrent.Future

class RegressionResultMongoAsyncCrudRepo @Inject()(
    @Assisted dataSetId : String,
    dictionaryRepo: DictionaryRootRepo
  ) extends DictionarySubordinateMongoAsyncCrudRepo[RegressionResult, BSONObjectID]("regressionResults", dataSetId, dictionaryRepo) {

  private val identity = RegressionResultIdentity

  override def save(entity: RegressionResult): Future[BSONObjectID] = {
    val initializedId = identity.of(entity).getOrElse(BSONObjectID.generate)
    super.save(identity.set(entity, initializedId))
  }
}